const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const axios = require('axios');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

// ==========================================
// 1. DATABASE SETUP (MongoDB Atlas)
// ==========================================
mongoose.connect(process.env.MONGO_URI)
  .then(() => console.log("🚀 MongoDB Atlas connected successfully!"))
  .catch((err) => console.error("❌ MongoDB connection error:", err));

// Schema to log every time a user triggers a backup sync
const SyncLogSchema = new mongoose.Schema({
  userId: String,
  playlistName: String,
  trackCount: Number,
  s3FileName: String,
  timestamp: { type: Date, default: Date.now }
});
const SyncLog = mongoose.model('SyncLog', SyncLogSchema);

// ==========================================
// 2. AWS HARDWARE CONNECTIONS (S3 & SNS)
// ==========================================
const awsConfig = {
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    sessionToken: process.env.AWS_SESSION_TOKEN // Required for AWS Academy Labs
  }
};
const s3Client = new S3Client(awsConfig);
const snsClient = new SNSClient(awsConfig);

// ==========================================
// FUNCTIONALITY 1: LIVE SPOTIFY SEARCH
// ==========================================
app.get('/api/search', async (req, res) => {
  const { query } = req.query;
  if (!query) return res.status(400).json({ error: "Missing search text query" });

  try {
    // A. Exchange Client ID & Secret for a temporary Spotify access token
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

    // B. Call Spotify search with the token
    const searchResponse = await axios.get(`https://api.spotify.com/v1/search?q=${encodeURIComponent(query)}&type=track&limit=5`, {
      headers: { 'Authorization': `Bearer ${spotifyToken}` }
    });

    // C. Clean up information to send to the frontend UI
    const tracks = searchResponse.data.tracks.items.map(track => ({
      id: track.id,
      title: track.name,
      artist: track.artists[0].name,
      image: track.album.images[0]?.url || ""
    }));

    res.json(tracks);
  } catch (error) {
    console.error("Spotify API failure:", error.message);
    res.status(500).json({ error: "Failed to pull music data from Spotify" });
  }
});

// ==========================================
// FUNCTIONALITY 2: MULTI-CLOUD SYNC & BACKUP
// ==========================================
app.post('/api/playlist/sync', async (req, res) => {
  const { userId, playlistName, tracks } = req.body;

  if (!userId || !playlistName || !tracks || !tracks.length) {
    return res.status(400).json({ error: "Missing required app data payload parameters" });
  }

  try {
    const fileName = `backups/user-${userId}/playlist-${Date.now()}.json`;

    // CLOUD TARGET A: Amazon S3 (Save physical backup file)
    await s3Client.send(new PutObjectCommand({
      Bucket: process.env.S3_BUCKET_NAME,
      Key: fileName,
      Body: JSON.stringify({ playlistName, tracks, syncedBy: userId }, null, 2),
      ContentType: 'application/json'
    }));
    console.log("-> S3 File written successfully.");

    // CLOUD TARGET B: MongoDB Atlas (Log structural database entry)
    const newLog = new SyncLog({
      userId,
      playlistName,
      trackCount: tracks.length,
      s3FileName: fileName
    });
    const databaseRecord = await newLog.save();
    console.log("-> MongoDB Transaction logged successfully.");

    // CLOUD TARGET C: AWS SNS (Broadcast event notification)
    const notificationText = `Success! Your app playlist "${playlistName}" has been safely backed up.\n\n` +
                             `• Songs saved: ${tracks.length}\n` +
                             `• Database Record ID: ${databaseRecord._id}\n` +
                             `• S3 File Target: ${fileName}\n\n` +
                             `All integrations are fully working!`;

    await snsClient.send(new PublishCommand({
      TopicArn: process.env.SNS_TOPIC_ARN,
      Subject: "Cloud Music Pipeline Success",
      Message: notificationText
    }));
    console.log("-> AWS SNS notification alert dispatched.");

    // Send complete success confirmation back to browser
    res.json({
      success: true,
      message: "Synced completely across MongoDB, S3, and SNS Notification!",
      mongoId: databaseRecord._id,
      s3Path: fileName
    });

  } catch (error) {
    console.error("Core Pipeline error:", error);
    res.status(500).json({ error: "Cloud sync failed to execute completely", details: error.message });
  }
});

// Run Backend Server Locally
const PORT = 5000;
app.listen(PORT, () => console.log(`🎯 Local compute active on port ${PORT}`));