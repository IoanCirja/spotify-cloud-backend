const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const axios = require('axios');
const jwt = require('jsonwebtoken');
const jwksClient = require('jwks-rsa');
const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { CloudWatchClient, PutMetricDataCommand } = require('@aws-sdk/client-cloudwatch');
require('dotenv').config();

const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json());

mongoose.connect(process.env.MONGO_URI)
  .then(() => console.log("MongoDB Atlas pipeline connected successfully!"))
  .catch((err) => console.error("MongoDB connection target error:", err));

const SyncLogSchema = new mongoose.Schema({
  userId: String,
  playlistName: String,
  trackCount: Number,
  s3FileName: String,
  timestamp: { type: Date, default: Date.now }
});
const SyncLog = mongoose.model('SyncLog', SyncLogSchema);

const awsConfig = {
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    sessionToken: process.env.AWS_SESSION_TOKEN
  }
};
const s3Client = new S3Client(awsConfig);
const snsClient = new SNSClient(awsConfig);
const cloudwatchClient = new CloudWatchClient(awsConfig);

const streamToString = (stream) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });

const client = jwksClient({
  jwksUri: `https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com`
});

function getKey(header, callback) {
  client.getSigningKey(header.kid, function(err, key) {
    var signingKey = key.publicKey || key.rsaPublicKey;
    callback(null, signingKey);
  });
}

const validateFirebaseToken = (req, res, next) => {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: "Access Denied. Token missing." });
  }
  const token = authHeader.split(' ')[1];

  jwt.verify(token, getKey, {
    audience: "playlister-5e329",
    issuer: `https://securetoken.google.com/playlister-5e329`,
    algorithms: ['RS256']
  }, (err, decodedToken) => {
    if (err) return res.status(403).json({ error: "Invalid token verification." });
    req.user = decodedToken;
    next();
  });
};

app.get('/api/search', async (req, res) => {
  const { query } = req.query;
  if (!query) return res.status(400).json({ error: "Missing query parameter." });

  try {
    const tokenResponse = await axios.post(
      'https://accounts.spotify.com/api/token',
      'grant_type=client_credentials',
      {
        headers: {
          'Authorization': 'Basic ' + Buffer.from(`${process.env.SPOTIFY_CLIENT_ID}:${process.env.SPOTIFY_CLIENT_SECRET}`).toString('base64'),
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      }
    );
    const spotifyToken = tokenResponse.data.access_token;

    const searchResponse = await axios.get(`https://api.spotify.com/v1/search?q=$${encodeURIComponent(query)}&type=track&limit=5`, {
      headers: { 'Authorization': `Bearer ${spotifyToken}` }
    });

    const tracks = searchResponse.data.tracks.items.map(track => ({
      id: track.id,
      title: track.name,
      artist: track.artists[0].name,
      image: track.album.images[0]?.url || ""
    }));

    res.json(tracks);
  } catch (error) {
    res.status(500).json({ error: "Spotify data integration error." });
  }
});

app.post('/api/playlist/sync', validateFirebaseToken, async (req, res) => {
  const { playlistName, tracks } = req.body;
  const userId = req.user.user_id;

  if (!playlistName || !tracks || !tracks.length) {
    return res.status(400).json({ error: "Malformed request payload parameters." });
  }

  const startTime = Date.now();

  try {
    const fileName = `backups/user-${userId}/playlist-${Date.now()}.json`;

    await s3Client.send(new PutObjectCommand({
      Bucket: process.env.S3_BUCKET_NAME,
      Key: fileName,
      Body: JSON.stringify({ playlistName, tracks, backedUpBy: userId }, null, 2),
      ContentType: 'application/json'
    }));

    const newLog = new SyncLog({
      userId,
      playlistName,
      trackCount: tracks.length,
      s3FileName: fileName
    });
    const savedLog = await newLog.save();

    const alertBody = `Success Notification Triggered!\n\n` +
                      `• Playlist Target Saved: ${playlistName}\n` +
                      `• Target Track Entries Count: ${tracks.length}\n` +
                      `• DB Object ID: ${savedLog._id}\n\n`;

    await snsClient.send(new PublishCommand({
      TopicArn: process.env.SNS_TOPIC_ARN,
      Subject: "Cloud Music Stack Sync",
      Message: alertBody
    }));

    const durationMs = Date.now() - startTime; 

    await cloudwatchClient.send(new PutMetricDataCommand({
      Namespace: "MusicCuratorApp/Infrastructure",
      MetricData: [
        {
          MetricName: "PipelineProcessingLatency",
          Dimensions: [{ Name: "Environment", Value: "Production" }],
          Unit: "Milliseconds",
          Value: durationMs
        }
      ]
    }));
    console.log(`-> Telemetry logged to CloudWatch. Duration: ${durationMs}ms`);

    res.json({ 
      success: true, 
      message: "Multi-Cloud Loop verified successfully.", 
      mongoId: savedLog._id,
      latency: durationMs
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Cloud sync pipeline trace error." });
  }
});

app.get('/api/history', validateFirebaseToken, async (req, res) => {
  try {
    const records = await SyncLog.find({ userId: req.user.user_id }).sort({ timestamp: -1 });
    res.json(records);
  } catch (err) {
    res.status(500).json({ error: "Failed to load database logs." });
  }
});

app.get('/api/analytics', validateFirebaseToken, async (req, res) => {
  try {
    const userId = req.user.user_id;
    const pipelineResult = await SyncLog.aggregate([
      { $match: { userId: userId } },
      {
        $group: {
          _id: "$userId",
          totalSyncs: { $sum: 1 },
          totalTracksBackedUp: { $sum: "$trackCount" },
          favoritePlaylist: { $first: "$playlistName" }
        }
      }
    ]);
    const metrics = pipelineResult[0] || { totalSyncs: 0, totalTracksBackedUp: 0, favoritePlaylist: "N/A" };
    res.json(metrics);
  } catch (err) {
    res.status(500).json({ error: "Aggregation pipeline trace failure." });
  }
});

app.get('/api/history/restore/:id', validateFirebaseToken, async (req, res) => {
  try {
    const record = await SyncLog.findOne({ _id: req.params.id, userId: req.user.user_id });
    if (!record) return res.status(404).json({ error: "Backup target not found." });

    const s3Response = await s3Client.send(new GetObjectCommand({
      Bucket: process.env.S3_BUCKET_NAME,
      Key: record.s3FileName
    }));

    const rawFileContent = await streamToString(s3Response.Body);
    const backupData = JSON.parse(rawFileContent);

    res.json({
      success: true,
      playlistName: backupData.playlistName,
      tracks: backupData.tracks
    });
  } catch (error) {
    res.status(500).json({ error: "Failed to pull data from S3." });
  }
});

app.delete('/api/history/:id', validateFirebaseToken, async (req, res) => {
  try {
    await SyncLog.deleteOne({ _id: req.params.id, userId: req.user.user_id });
    res.json({ success: true, message: "Dropped record successfully." });
  } catch (err) {
    res.status(500).json({ error: "Failed to execute deletion trace." });
  }
});

const PORT = 5000;
app.listen(PORT, () => console.log(` Compute server running on port ${PORT}`));