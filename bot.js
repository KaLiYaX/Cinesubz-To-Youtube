// Load environment variables
require('dotenv').config();

const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');
const { google } = require('googleapis');
const readline = require('readline');

// Configuration
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const API_KEY = process.env.API_KEY || '1b899858fd185941';
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'kalindu_gaweshana';

// YouTube Configuration
const YOUTUBE_CLIENT_ID = process.env.YOUTUBE_CLIENT_ID;
const YOUTUBE_CLIENT_SECRET = process.env.YOUTUBE_CLIENT_SECRET;
const YOUTUBE_REDIRECT_URI = process.env.YOUTUBE_REDIRECT_URI || 'http://127.0.0.1:3000';

// Data file paths
const DATA_DIR = path.join(__dirname, 'data');
const HISTORY_FILE = path.join(DATA_DIR, 'processed_movies.json');
const ANALYTICS_FILE = path.join(DATA_DIR, 'analytics.json');
const CACHE_DIR = path.join(DATA_DIR, 'cache');
const TOKEN_PATH = path.join(DATA_DIR, 'youtube_token.json');

// Increase limits - support for long videos (up to 12 hours)
const MAX_VIDEO_SIZE = 256 * 1024 * 1024 * 1024; // 256GB (YouTube max)
const DOWNLOAD_TIMEOUT = 120 * 60 * 1000; // 120 minutes for very large files
const UPLOAD_CHUNK_SIZE = 256 * 1024 * 1024; // 256MB chunks for resumable upload

if (!TELEGRAM_TOKEN) {
  console.error('❌ Missing TELEGRAM_TOKEN!');
  process.exit(1);
}

if (!YOUTUBE_CLIENT_ID || !YOUTUBE_CLIENT_SECRET) {
  console.error('❌ Missing YouTube credentials! Please set YOUTUBE_CLIENT_ID and YOUTUBE_CLIENT_SECRET in .env');
  process.exit(1);
}

const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });

let ADMIN_ID = null;
const videoQueue = [];
let processedMovies = new Set();
let analytics = {
  totalMovies: 0,
  successfulPosts: 0,
  failedPosts: 0,
  totalSize: 0,
  duplicatesSkipped: 0,
  startTime: Date.now(),
  lastSaved: null
};
const userSessions = new Map();
const activeDownloads = new Map();
let currentProcessing = null;
let youtubeAuth = null;

// ============================================
// YOUTUBE AUTHENTICATION
// ============================================

async function getYouTubeAuth() {
  const oauth2Client = new google.auth.OAuth2(
    YOUTUBE_CLIENT_ID,
    YOUTUBE_CLIENT_SECRET,
    YOUTUBE_REDIRECT_URI
  );

  try {
    const token = await fs.readFile(TOKEN_PATH, 'utf8');
    oauth2Client.setCredentials(JSON.parse(token));
    console.log('✅ YouTube auth loaded from file');
    return oauth2Client;
  } catch (error) {
    console.log('🔐 YouTube auth not found, need to authenticate...');
    return await getNewYouTubeToken(oauth2Client);
  }
}

async function getNewYouTubeToken(oauth2Client) {
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: ['https://www.googleapis.com/auth/youtube.upload']
  });

  console.log('\n========================================');
  console.log('🔐 YOUTUBE AUTHENTICATION REQUIRED');
  console.log('========================================\n');
  console.log('1. Open this URL in your browser:');
  console.log('\n' + authUrl + '\n');
  console.log('2. Authorize the application');
  console.log('3. Copy the code and paste it here\n');

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  return new Promise((resolve, reject) => {
    rl.question('Enter the authorization code: ', async (code) => {
      rl.close();
      
      try {
        const { tokens } = await oauth2Client.getToken(code);
        oauth2Client.setCredentials(tokens);
        
        await fs.writeFile(TOKEN_PATH, JSON.stringify(tokens, null, 2));
        console.log('✅ YouTube token saved successfully!');
        
        resolve(oauth2Client);
      } catch (error) {
        console.error('❌ Error getting YouTube token:', error.message);
        reject(error);
      }
    });
  });
}

// ============================================
// FILE MANAGEMENT
// ============================================

async function ensureDataDirectory() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    await fs.mkdir(CACHE_DIR, { recursive: true });
    console.log('📁 Data directory ready');
  } catch (error) {
    console.error('❌ Failed to create data directory:', error.message);
  }
}

async function loadProcessedMovies() {
  try {
    const data = await fs.readFile(HISTORY_FILE, 'utf8');
    const parsed = JSON.parse(data);
    processedMovies = new Set(parsed.movies || []);
    console.log(`📂 Loaded ${processedMovies.size} movies`);
  } catch (error) {
    if (error.code === 'ENOENT') {
      console.log('📂 Starting fresh');
      processedMovies = new Set();
    }
  }
}

async function saveProcessedMovies() {
  try {
    const data = {
      movies: Array.from(processedMovies),
      lastUpdated: new Date().toISOString(),
      count: processedMovies.size
    };
    await fs.writeFile(HISTORY_FILE, JSON.stringify(data, null, 2));
  } catch (error) {
    console.error('❌ Error saving history:', error.message);
  }
}

async function loadAnalytics() {
  try {
    const data = await fs.readFile(ANALYTICS_FILE, 'utf8');
    const parsed = JSON.parse(data);
    analytics = { ...analytics, ...parsed, startTime: parsed.startTime || Date.now() };
    console.log('📊 Analytics loaded');
  } catch (error) {
    if (error.code !== 'ENOENT') {
      console.error('❌ Error loading analytics:', error.message);
    }
  }
}

async function saveAnalytics() {
  try {
    analytics.lastSaved = new Date().toISOString();
    await fs.writeFile(ANALYTICS_FILE, JSON.stringify(analytics, null, 2));
  } catch (error) {
    console.error('❌ Error saving analytics:', error.message);
  }
}

setInterval(async () => {
  await saveProcessedMovies();
  await saveAnalytics();
}, 5 * 60 * 1000);

// ============================================
// PROGRESS BAR
// ============================================

function getProgressBar(percent) {
  const filled = Math.floor(percent / 10);
  const empty = 10 - filled;
  return '█'.repeat(filled) + '░'.repeat(empty);
}

// ============================================
// HELPER FUNCTIONS
// ============================================

function isAdmin(msg) {
  if (ADMIN_ID && msg.from.id === ADMIN_ID) return true;
  if (msg.from.username === ADMIN_USERNAME) {
    ADMIN_ID = msg.from.id;
    return true;
  }
  return false;
}

function getUserSession(userId) {
  if (!userSessions.has(userId)) {
    userSessions.set(userId, {
      searchResults: [],
      selectedMovie: null,
      movieData: null
    });
  }
  return userSessions.get(userId);
}

function isAlreadyProcessed(movieUrl) {
  return processedMovies.has(movieUrl);
}

function isInQueue(movieUrl) {
  return videoQueue.some(item => item.movieUrl === movieUrl);
}

// ============================================
// KEYBOARDS
// ============================================

const keyboards = {
  main: () => ({
    inline_keyboard: [
      [{ text: '🔍 Search Movie', callback_data: 'search_movie' }, { text: '📋 Queue', callback_data: 'view_queue' }],
      [{ text: '📊 Analytics', callback_data: 'analytics' }, { text: '❓ Help', callback_data: 'help' }]
    ]
  }),
  
  queueItem: (index) => ({
    inline_keyboard: [
      [{ text: '❌ Remove', callback_data: `queue_remove_${index}` }],
      [{ text: '🔙 Back', callback_data: 'view_queue' }]
    ]
  }),

  cancelResume: (taskId) => ({
    inline_keyboard: [
      [{ text: '⏸️ Pause', callback_data: `pause_${taskId}` }, { text: '❌ Cancel', callback_data: `cancel_${taskId}` }]
    ]
  }),

  resumeTask: (taskId) => ({
    inline_keyboard: [
      [{ text: '▶️ Resume', callback_data: `resume_${taskId}` }, { text: '❌ Cancel', callback_data: `cancel_${taskId}` }]
    ]
  }),

  alreadyProcessed: (movieUrl) => ({
    inline_keyboard: [
      [{ text: '✅ Yes, Repost', callback_data: `repost_confirm_${Buffer.from(movieUrl).toString('base64').substring(0, 50)}` }],
      [{ text: '❌ No, Cancel', callback_data: 'main_menu' }]
    ]
  })
};

// ============================================
// COMMANDS
// ============================================

bot.onText(/\/start/, (msg) => {
  if (!isAdmin(msg)) {
    return bot.sendMessage(msg.chat.id, '❌ Admin Only\n🔐 @' + ADMIN_USERNAME);
  }

  bot.sendMessage(msg.chat.id, `
👋 *Welcome ${msg.from.first_name}!*

🤖 *CineSubz Movie Bot*

✅ Search movies from CineSubz
✅ Multiple quality options
✅ Auto upload to YouTube 📺
✅ Support for long videos (up to 12 hours) 🎬
✅ Resumable uploads for large files 📤
✅ Progress tracking 📊
✅ Queue management 🗂️
✅ Cancel & Resume support ⏸️
✅ Repost processed movies 🔄

⚠️ *Note:* Your YouTube account must be verified to upload videos longer than 15 minutes.
Verify at: https://www.youtube.com/verify
  `, { parse_mode: 'Markdown', reply_markup: keyboards.main() });
});

bot.onText(/\/search (.+)/, async (msg, match) => {
  if (!isAdmin(msg)) return;
  
  const searchQuery = match[1];
  await handleSearch(msg.chat.id, msg.from.id, searchQuery);
});

bot.onText(/\/cancel/, async (msg) => {
  if (!isAdmin(msg)) return;
  
  if (currentProcessing) {
    currentProcessing.cancelled = true;
    bot.sendMessage(msg.chat.id, '⏸️ Cancelling current task...', { parse_mode: 'Markdown' });
  } else {
    bot.sendMessage(msg.chat.id, '❌ No active task to cancel', { parse_mode: 'Markdown' });
  }
});

bot.onText(/\/reauth/, async (msg) => {
  if (!isAdmin(msg)) return;
  
  bot.sendMessage(msg.chat.id, '🔐 Re-authenticating with YouTube...');
  
  try {
    await fs.unlink(TOKEN_PATH).catch(() => {});
    youtubeAuth = await getYouTubeAuth();
    bot.sendMessage(msg.chat.id, '✅ YouTube authentication successful!');
  } catch (error) {
    bot.sendMessage(msg.chat.id, '❌ Authentication failed: ' + error.message);
  }
});

bot.onText(/\/verify/, async (msg) => {
  if (!isAdmin(msg)) return;
  
  bot.sendMessage(msg.chat.id, `
📺 *YouTube Account Verification*

To upload videos longer than 15 minutes, you need to verify your YouTube account.

*Steps:*
1. Go to: https://www.youtube.com/verify
2. Follow the verification process
3. Once verified, you can upload videos up to 12 hours long

*Current Limits:*
• Unverified: Up to 15 minutes
• Verified: Up to 12 hours
• Max file size: 256 GB

After verification, your bot will automatically support longer videos!
  `, { parse_mode: 'Markdown' });
});

// ============================================
// CALLBACK HANDLER
// ============================================

bot.on('callback_query', async (query) => {
  const { message: msg, data, from } = query;
  if (!isAdmin(query)) return bot.answerCallbackQuery(query.id, { text: '❌ Admin only!' });
  
  bot.answerCallbackQuery(query.id);
  const session = getUserSession(from.id);

  try {
    if (data === 'main_menu') {
      await bot.editMessageText('*🏠 Main Menu*', {
        chat_id: msg.chat.id, message_id: msg.message_id,
        parse_mode: 'Markdown', reply_markup: keyboards.main()
      });
    }
    
    else if (data === 'search_movie') {
      await bot.editMessageText('🔍 *Search Movie*\n\nUse: /search <movie name>\n\nExample: /search Bad Newz', {
        chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '🔙 Back', callback_data: 'main_menu' }]] }
      });
    }
    
    else if (data.startsWith('select_')) {
      const index = parseInt(data.split('_')[1]);
      const movie = session.searchResults[index];
      
      if (!movie) {
        return bot.answerCallbackQuery(query.id, { text: '❌ Movie not found' });
      }
      
      session.selectedMovie = movie;
      
      const loadingMsg = await bot.sendMessage(msg.chat.id, 
        `⏳ *Fetching Movie Details*\n\n${getProgressBar(0)} 0%\n\nConnecting to API...`, 
        { parse_mode: 'Markdown' }
      );
      
      try {
        await bot.editMessageText(
          `⏳ *Fetching Movie Details*\n\n${getProgressBar(20)} 20%\n\nRequesting movie info...`,
          { chat_id: msg.chat.id, message_id: loadingMsg.message_id, parse_mode: 'Markdown' }
        );
        
        const infoUrl = `https://api-dark-shan-yt.koyeb.app/movie/cinesubz-info?url=${encodeURIComponent(movie.link)}&apikey=${API_KEY}`;
        const response = await axios.get(infoUrl);
        
        await bot.editMessageText(
          `⏳ *Fetching Movie Details*\n\n${getProgressBar(60)} 60%\n\nProcessing response...`,
          { chat_id: msg.chat.id, message_id: loadingMsg.message_id, parse_mode: 'Markdown' }
        );
        
        if (!response.data.status || !response.data.data) {
          throw new Error('Failed to fetch movie details');
        }
        
        const movieData = response.data.data;
        session.movieData = movieData;
        
        await bot.editMessageText(
          `⏳ *Fetching Movie Details*\n\n${getProgressBar(90)} 90%\n\nPreparing display...`,
          { chat_id: msg.chat.id, message_id: loadingMsg.message_id, parse_mode: 'Markdown' }
        );
        
        let message = `🎬 *${movieData.title}*\n\n`;
        message += `⭐ Rating: ${movieData.rating}\n`;
        message += `📅 Year: ${movieData.year}\n`;
        message += `⏱️ Duration: ${movieData.duration}\n`;
        message += `🗣️ Language: ${movieData.tag}\n`;
        message += `🎥 ${movieData.directors}\n\n`;
        message += `📥 *Select Quality:*\n`;
        
        const qualityButtons = movieData.downloads.map((download, idx) => [{
          text: `${download.quality} - ${download.size}`,
          callback_data: `download_${idx}`
        }]);
        
        await bot.deleteMessage(msg.chat.id, loadingMsg.message_id);
        
        const moviePoster = movie.image || movieData.image;
        
        if (moviePoster && moviePoster !== 'https://cinesubz.lk/wp-content/themes/zetaflix/assets/img/no/zt_backdrop.png') {
          await bot.sendPhoto(msg.chat.id, moviePoster, {
            caption: message,
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: qualityButtons }
          });
        } else {
          bot.sendMessage(msg.chat.id, message, {
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: qualityButtons }
          });
        }
        
      } catch (error) {
        console.error('Info fetch error:', error);
        bot.editMessageText('❌ Error fetching movie details.', {
          chat_id: msg.chat.id,
          message_id: loadingMsg.message_id
        });
      }
    }
    
    else if (data.startsWith('download_')) {
      const index = parseInt(data.split('_')[1]);
      const download = session.movieData?.downloads[index];
      
      if (!download) {
        return bot.answerCallbackQuery(query.id, { text: '❌ Invalid selection' });
      }
      
      const movieData = session.movieData;
      
      if (isAlreadyProcessed(session.selectedMovie.link)) {
        return bot.sendMessage(msg.chat.id, 
          `⚠️ *Already Processed*\n\n🎬 ${movieData.title}\n\nThis movie was already uploaded to YouTube.\n\n*Do you want to repost it?*`,
          { parse_mode: 'Markdown', reply_markup: keyboards.alreadyProcessed(session.selectedMovie.link) }
        );
      }
      
      await fetchDownloadLinksAndQueue(msg.chat.id, session, download, movieData);
    }

    else if (data.startsWith('repost_confirm_')) {
      const movieData = session.movieData;
      
      bot.sendMessage(msg.chat.id, 
        `🔄 *Reposting Movie*\n\n🎬 ${movieData.title}\n\nSelect quality to repost:`,
        { 
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: movieData.downloads.map((d, idx) => [{
              text: `${d.quality} - ${d.size}`,
              callback_data: `repost_quality_${idx}`
            }])
          }
        }
      );
    }

    else if (data.startsWith('repost_quality_')) {
      const index = parseInt(data.split('_')[2]);
      const download = session.movieData?.downloads[index];
      const movieData = session.movieData;
      
      await fetchDownloadLinksAndQueue(msg.chat.id, session, download, movieData, true);
    }
    
    else if (data.startsWith('source_')) {
      const index = parseInt(data.split('_')[1]);
      const selectedSource = session.downloadData?.download[index];
      
      if (!selectedSource) {
        return bot.answerCallbackQuery(query.id, { text: '❌ Invalid source' });
      }
      
      const movieData = session.movieData;
      const downloadData = session.downloadData;
      
      const taskId = Date.now().toString();
      
      videoQueue.push({
        taskId: taskId,
        chatId: msg.chat.id,
        movieUrl: session.selectedMovie.link,
        movieData: movieData,
        download: {
          quality: downloadData.title,
          size: downloadData.size,
          link: selectedSource.url
        },
        source: selectedSource.name,
        status: 'pending',
        addedAt: Date.now(),
        paused: false,
        cancelled: false
      });
      
      bot.sendMessage(msg.chat.id, 
        `✅ *Added to Queue*\n\n🎬 ${movieData.title}\n💾 ${downloadData.size}\n📦 Source: ${selectedSource.name.toUpperCase()}\n\n⚠️ For videos longer than 15 minutes, ensure your YouTube account is verified.\nUse /verify for info.`,
        { parse_mode: 'Markdown', reply_markup: keyboards.main() }
      );
      
      if (!videoQueue.some(v => v.status === 'processing')) {
        processQueue();
      }
    }
    
    else if (data.startsWith('pause_')) {
      if (currentProcessing) {
        currentProcessing.paused = true;
        bot.answerCallbackQuery(query.id, { text: '⏸️ Task paused' });
      }
    }

    else if (data.startsWith('resume_')) {
      if (currentProcessing) {
        currentProcessing.paused = false;
        bot.answerCallbackQuery(query.id, { text: '▶️ Task resumed' });
        processQueue();
      }
    }

    else if (data.startsWith('cancel_')) {
      if (currentProcessing) {
        currentProcessing.cancelled = true;
        bot.answerCallbackQuery(query.id, { text: '❌ Task cancelled' });
      }
    }
    
    else if (data === 'view_queue') {
      if (videoQueue.length === 0) {
        await bot.editMessageText('📭 *Queue Empty*', {
          chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '🔍 Search', callback_data: 'search_movie' }]] }
        });
      } else {
        let text = `📋 *Queue* (${videoQueue.length})\n\n`;
        const buttons = [];
        
        videoQueue.forEach((item, i) => {
          let status = '⏸️';
          if (item.status === 'processing') status = item.paused ? '⏸️' : '⏳';
          else if (item.status === 'completed') status = '✅';
          else if (item.cancelled) status = '❌';
          
          text += `${status} ${i + 1}. ${(item.movieData.title || 'Processing...').substring(0, 30)}...\n`;
          buttons.push([{ text: `${i + 1}. ${(item.movieData.title || '...').substring(0, 20)}`, callback_data: `queue_item_${i}` }]);
        });
        
        buttons.push([{ text: '🔄 Refresh', callback_data: 'view_queue' }, { text: '🔙 Back', callback_data: 'main_menu' }]);
        await bot.editMessageText(text, {
          chat_id: msg.chat.id, message_id: msg.message_id,
          parse_mode: 'Markdown', reply_markup: { inline_keyboard: buttons }
        });
      }
    }
    
    else if (data.startsWith('queue_item_')) {
      const index = parseInt(data.split('_')[2]);
      const item = videoQueue[index];
      if (item) {
        await bot.editMessageText(
          `🎬 *Queue #${index + 1}*\n\n` +
          `📝 ${item.movieData.title}\n` +
          `💾 Size: ${item.download.size}\n` +
          `📦 Source: ${item.source || 'N/A'}\n` +
          `📊 Status: ${item.status}${item.paused ? ' (Paused)' : ''}${item.cancelled ? ' (Cancelled)' : ''}\n` +
          `⏰ ${new Date(item.addedAt).toLocaleTimeString()}`,
          {
            chat_id: msg.chat.id, message_id: msg.message_id,
            parse_mode: 'Markdown', reply_markup: keyboards.queueItem(index)
          }
        );
      }
    }
    
    else if (data.startsWith('queue_remove_')) {
      const index = parseInt(data.split('_')[2]);
      const removed = videoQueue.splice(index, 1)[0];
      if (removed.taskId === currentProcessing?.taskId) {
        currentProcessing.cancelled = true;
      }
      await bot.editMessageText(`✅ *Removed*\n\n${removed.movieData.title}`, {
        chat_id: msg.chat.id, message_id: msg.message_id,
        parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '🔙 Queue', callback_data: 'view_queue' }]] }
      });
    }
    
    else if (data === 'analytics') {
      const uptime = Math.floor((Date.now() - analytics.startTime) / 60000);
      const avgSize = analytics.totalMovies > 0 ? (analytics.totalSize / analytics.totalMovies).toFixed(2) : 0;
      const successRate = analytics.totalMovies > 0 ? ((analytics.successfulPosts / analytics.totalMovies) * 100).toFixed(1) : 0;
      
      await bot.editMessageText(`
📊 *Analytics*

🎬 Total Movies: ${analytics.totalMovies}
✅ Success: ${analytics.successfulPosts}
❌ Failed: ${analytics.failedPosts}
🔍 Duplicates: ${analytics.duplicatesSkipped}
📈 Success Rate: ${successRate}%

💾 Total Size: ${(analytics.totalSize / 1024).toFixed(2)} GB
📏 Avg Size: ${avgSize} MB

⏱️ Uptime: ${uptime} min
📋 Queue: ${videoQueue.length}
🗂️ History: ${processedMovies.size}
      `, {
        chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '🔄 Refresh', callback_data: 'analytics' }, { text: '🔙 Back', callback_data: 'main_menu' }]] }
      });
    }
    
    else if (data === 'help') {
      await bot.editMessageText(`
❓ *Help*

*Search:*
🔍 /search <movie name>
Example: /search Bad Newz

*Commands:*
/cancel - Stop current download/upload
/reauth - Re-authenticate YouTube
/verify - YouTube verification info

*Features:*
🎬 Multiple quality options
📦 Multiple download sources
📺 Auto upload to YouTube
🎞️ Support for long videos (up to 12 hours)
📤 Resumable uploads for large files
📊 Real-time progress tracking
📋 Queue management
⏸️ Pause & Resume support
❌ Cancel anytime
🔄 Repost processed movies
💾 Persistent storage
🔍 Duplicate detection

*Important Notes:*
⚠️ Videos longer than 15 minutes require a verified YouTube account
⚠️ Max video length: 12 hours (verified accounts)
⚠️ Max file size: 256 GB

Admin: @${ADMIN_USERNAME}
      `, {
        chat_id: msg.chat.id, message_id: msg.message_id,
        parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '🔙 Back', callback_data: 'main_menu' }]] }
      });
    }
    
  } catch (error) {
    console.error('Callback error:', error.message);
  }
});

// ============================================
// HELPER: FETCH DOWNLOAD LINKS AND ADD TO QUEUE
// ============================================

async function fetchDownloadLinksAndQueue(chatId, session, download, movieData, isRepost = false) {
  const fetchingMsg = await bot.sendMessage(chatId,
    `⏳ *Fetching Download Links*\n\n${getProgressBar(0)} 0%\n\nPreparing request...`,
    { parse_mode: 'Markdown' }
  );
  
  try {
    await bot.editMessageText(
      `⏳ *Fetching Download Links*\n\n${getProgressBar(30)} 30%\n\nConnecting to API...`,
      { chat_id: chatId, message_id: fetchingMsg.message_id, parse_mode: 'Markdown' }
    );
    
    const downloadUrl = `https://api-dark-shan-yt.koyeb.app/movie/cinesubz-download?url=${encodeURIComponent(download.link)}&apikey=${API_KEY}`;
    const downloadResponse = await axios.get(downloadUrl);
    
    await bot.editMessageText(
      `⏳ *Fetching Download Links*\n\n${getProgressBar(70)} 70%\n\nProcessing links...`,
      { chat_id: chatId, message_id: fetchingMsg.message_id, parse_mode: 'Markdown' }
    );
    
    if (!downloadResponse.data.status || !downloadResponse.data.data) {
      throw new Error('Failed to fetch download links');
    }
    
    const downloadData = downloadResponse.data.data;
    
    await bot.editMessageText(
      `⏳ *Fetching Download Links*\n\n${getProgressBar(100)} 100%\n\nLinks ready!`,
      { chat_id: chatId, message_id: fetchingMsg.message_id, parse_mode: 'Markdown' }
    );
    
    await new Promise(resolve => setTimeout(resolve, 500));
    
    let optionsMessage = `📥 *Download Options*\n\n`;
    optionsMessage += `🎬 ${movieData.title.substring(0, 40)}...\n`;
    optionsMessage += `💾 ${downloadData.size}\n`;
    if (isRepost) optionsMessage += `🔄 Reposting\n`;
    optionsMessage += `\n*Select Download Source:*\n`;
    
    const sourceButtons = downloadData.download.map((src, idx) => {
      let emoji = '📦';
      if (src.name === 'gdrive') emoji = '📁';
      else if (src.name === 'cloud') emoji = '☁️';
      else if (src.name === 'pix') emoji = '🎨';
      else if (src.name === 'telegram') emoji = '✈️';
      
      return [{
        text: `${emoji} ${src.name.toUpperCase()}`,
        callback_data: `source_${idx}`
      }];
    });
    
    session.downloadData = downloadData;
    
    await bot.deleteMessage(chatId, fetchingMsg.message_id);
    
    bot.sendMessage(chatId, optionsMessage, {
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [...sourceButtons, [{ text: '🔙 Back', callback_data: `select_${session.searchResults.indexOf(session.selectedMovie)}` }]] }
    });
    
  } catch (error) {
    console.error('Download links fetch error:', error);
    bot.editMessageText('❌ Error fetching download links.', {
      chat_id: chatId,
      message_id: fetchingMsg.message_id
    });
  }
}

// ============================================
// SEARCH HANDLER
// ============================================

async function handleSearch(chatId, userId, searchQuery) {
  const loadingMsg = await bot.sendMessage(chatId, 
    `🔍 *Searching*\n\n${getProgressBar(0)} 0%\n\nInitializing search...`,
    { parse_mode: 'Markdown' }
  );
  
  try {
    await bot.editMessageText(
      `🔍 *Searching*\n\n${getProgressBar(30)} 30%\n\nQuerying API...`,
      { chat_id: chatId, message_id: loadingMsg.message_id, parse_mode: 'Markdown' }
    );
    
    const searchUrl = `https://api-dark-shan-yt.koyeb.app/movie/cinesubz-search?q=${encodeURIComponent(searchQuery)}&apikey=${API_KEY}`;
    const response = await axios.get(searchUrl);
    
    await bot.editMessageText(
      `🔍 *Searching*\n\n${getProgressBar(70)} 70%\n\nProcessing results...`,
      { chat_id: chatId, message_id: loadingMsg.message_id, parse_mode: 'Markdown' }
    );
    
    if (!response.data.status || !response.data.data || response.data.data.length === 0) {
      bot.editMessageText('❌ No movies found for your search.', {
        chat_id: chatId,
        message_id: loadingMsg.message_id
      });
      return;
    }
    
    await bot.editMessageText(
      `🔍 *Searching*\n\n${getProgressBar(100)} 100%\n\nLoading results...`,
      { chat_id: chatId, message_id: loadingMsg.message_id, parse_mode: 'Markdown' }
    );
    
    const movies = response.data.data.slice(0, 10);
    const session = getUserSession(userId);
    session.searchResults = movies;
    
    const keyboard = movies.map((movie, index) => [{
      text: `${movie.title} (${movie.rating}⭐)`,
      callback_data: `select_${index}`
    }]);
    
    bot.editMessageText('📽️ Select a movie:', {
      chat_id: chatId,
      message_id: loadingMsg.message_id,
      reply_markup: {
        inline_keyboard: keyboard
      }
    });
    
  } catch (error) {
    console.error('Search error:', error);
    bot.editMessageText('❌ Error searching for movies. Please try again.', {
      chat_id: chatId,
      message_id: loadingMsg.message_id
    });
  }
}

// ============================================
// QUEUE & PROCESSING
// ============================================

async function processQueue() {
  const next = videoQueue.find(v => v.status === 'pending' && !v.cancelled && !v.paused);
  if (!next) return;
  
  next.status = 'processing';
  currentProcessing = next;
  
  try {
    await processMovie(next);
    
    if (!next.cancelled) {
      next.status = 'completed';
      processedMovies.add(next.movieUrl);
      await saveProcessedMovies();
      await saveAnalytics();
    }
  } catch (error) {
    if (!next.cancelled) {
      next.status = 'failed';
      next.error = error.message;
      await saveAnalytics();
    }
  }
  
  videoQueue.splice(videoQueue.indexOf(next), 1);
  currentProcessing = null;
  
  setTimeout(processQueue, 2000);
}

async function processMovie(item) {
  const { chatId, movieData, download, taskId } = item;
  let progressMsg;
  let tempFilePath = null;
  
  try {
    progressMsg = await bot.sendMessage(chatId, 
      `⏳ *Starting...*\n\n🎬 ${movieData.title}\n\n${getProgressBar(0)} 0%`, 
      { parse_mode: 'Markdown', reply_markup: keyboards.cancelResume(taskId) }
    );
    
    analytics.totalMovies++;
    
    // Check for pause/cancel
    if (item.cancelled) throw new Error('Task cancelled by user');
    while (item.paused) {
      await bot.editMessageText(
        `⏸️ *Paused*\n\n🎬 ${movieData.title.substring(0, 40)}...\n💾 ${download.size}\n\nTask is paused`,
        { chat_id: chatId, message_id: progressMsg.message_id, parse_mode: 'Markdown', reply_markup: keyboards.resumeTask(taskId) }
      ).catch(() => {});
      await new Promise(resolve => setTimeout(resolve, 2000));
      if (item.cancelled) throw new Error('Task cancelled by user');
    }
    
    await bot.editMessageText(
      `📥 *Downloading*\n\n🎬 ${movieData.title.substring(0, 40)}...\n💾 ${download.size}\n\n${getProgressBar(10)} 10%`,
      { chat_id: chatId, message_id: progressMsg.message_id, parse_mode: 'Markdown', reply_markup: keyboards.cancelResume(taskId) }
    );
    
    // Download video with cancel support
    const cancelToken = axios.CancelToken.source();
    activeDownloads.set(taskId, cancelToken);
    
    const cancelCheckInterval = setInterval(() => {
      if (item.cancelled) {
        cancelToken.cancel('Download cancelled by user');
        clearInterval(cancelCheckInterval);
      }
    }, 500);
    
    const videoResponse = await axios({
      method: 'GET',
      url: download.link,
      responseType: 'arraybuffer',
      maxContentLength: MAX_VIDEO_SIZE,
      maxBodyLength: MAX_VIDEO_SIZE,
      timeout: DOWNLOAD_TIMEOUT,
      cancelToken: cancelToken.token,
      onDownloadProgress: async (progressEvent) => {
        while (item.paused && !item.cancelled) {
          await bot.editMessageText(
            `⏸️ *Download Paused*\n\n🎬 ${movieData.title.substring(0, 40)}...\n💾 ${download.size}\n\nDownload paused at ${Math.floor((progressEvent.loaded / progressEvent.total) * 100)}%`,
            { chat_id: chatId, message_id: progressMsg.message_id, parse_mode: 'Markdown', reply_markup: keyboards.resumeTask(taskId) }
          ).catch(() => {});
          await new Promise(resolve => setTimeout(resolve, 2000));
        }
        
        if (item.cancelled) {
          cancelToken.cancel('Download cancelled by user');
          return;
        }
        
        const percent = Math.floor((progressEvent.loaded / progressEvent.total) * 100);
        
        try {
          await bot.editMessageText(
            `📥 *Downloading*\n\n🎬 ${movieData.title.substring(0, 40)}...\n💾 ${download.size}\n\n${getProgressBar(percent)} ${percent}%`,
            { chat_id: chatId, message_id: progressMsg.message_id, parse_mode: 'Markdown', reply_markup: keyboards.cancelResume(taskId) }
          );
        } catch {}
      }
    });
    
    clearInterval(cancelCheckInterval);
    activeDownloads.delete(taskId);
    
    if (item.cancelled) throw new Error('Task cancelled by user');
    
    const fileSizeMB = (videoResponse.data.byteLength / (1024 * 1024)).toFixed(2);
    analytics.totalSize += parseFloat(fileSizeMB);
    
    // Save video to temp file for YouTube upload
    tempFilePath = path.join(CACHE_DIR, `${taskId}.mp4`);
    await fs.writeFile(tempFilePath, Buffer.from(videoResponse.data));
    
    await bot.editMessageText(
      `✅ *Download Complete*\n\n🎬 ${movieData.title.substring(0, 40)}...\n💾 ${download.size}\n\n${getProgressBar(100)} 100%\n\n⏳ Preparing YouTube upload...`,
      { chat_id: chatId, message_id: progressMsg.message_id, parse_mode: 'Markdown' }
    );
    
    await new Promise(resolve => setTimeout(resolve, 1500));
    
    if (item.cancelled) throw new Error('Task cancelled by user');
    
    // Upload to YouTube
    await bot.editMessageText(
      `📺 *Starting YouTube Upload*\n\n🎬 ${movieData.title.substring(0, 40)}...\n💾 ${download.size}\n\n${getProgressBar(0)} 0%\n\nInitializing upload...`,
      { chat_id: chatId, message_id: progressMsg.message_id, parse_mode: 'Markdown', reply_markup: keyboards.cancelResume(taskId) }
    );
    
    const uploadResult = await uploadVideoToYouTube(tempFilePath, movieData, chatId, progressMsg.message_id, item);
    
    // Clean up temp file
    await fs.unlink(tempFilePath).catch(() => {});
    tempFilePath = null;
    
    if (item.cancelled) throw new Error('Task cancelled by user');
    
    if (!uploadResult || !uploadResult.success) {
      throw new Error('YouTube upload failed');
    }
    
    analytics.successfulPosts++;
    
    const videoLink = uploadResult.id ? `\n📺 Video: https://youtu.be/${uploadResult.id}` : '';
    
    await bot.editMessageText(
      `✅ *Posted Successfully!*\n\n🎬 ${movieData.title.substring(0, 40)}...\n💾 ${download.size}${videoLink}\n\n${getProgressBar(100)} 100%`,
      {
        chat_id: chatId, message_id: progressMsg.message_id, parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '📊 Analytics', callback_data: 'analytics' }, { text: '🏠 Menu', callback_data: 'main_menu' }]] }
      }
    );
    
  } catch (error) {
    // Clean up temp file on error
    if (tempFilePath) {
      await fs.unlink(tempFilePath).catch(() => {});
    }
    
    if (error.message === 'Task cancelled by user' || axios.isCancel(error)) {
      console.log('❌ Task cancelled:', taskId);
      if (progressMsg) {
        try {
          await bot.editMessageText(
            `❌ *Task Cancelled*\n\n🎬 ${movieData.title.substring(0, 40)}...\n\nTask was cancelled by user`,
            {
              chat_id: chatId, message_id: progressMsg.message_id, parse_mode: 'Markdown',
              reply_markup: { inline_keyboard: [[{ text: '🏠 Menu', callback_data: 'main_menu' }]] }
            }
          );
        } catch {}
      }
      return;
    }
    
    analytics.failedPosts++;
    console.error('Process error:', error.message);
    
    if (progressMsg) {
      try {
        await bot.editMessageText(
          `❌ *Error*\n\n${error.message}`,
          {
            chat_id: chatId, message_id: progressMsg.message_id, parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '🔄 Retry', callback_data: 'search_movie' }]] }
          }
        );
      } catch {}
    }
    throw error;
  }
}

// ============================================
// YOUTUBE UPLOAD WITH RESUMABLE UPLOAD & PROGRESS
// ============================================

async function uploadVideoToYouTube(filePath, movieData, chatId, messageId, item) {
  try {
    const youtube = google.youtube({
      version: 'v3',
      auth: youtubeAuth
    });

    if (item && item.cancelled) throw new Error('Task cancelled by user');

    // Prepare video metadata
    const title = movieData.title.substring(0, 100); // YouTube title limit
    const description = `${movieData.title}

⭐ Rating: ${movieData.rating}
📅 Year: ${movieData.year}
⏱️ Duration: ${movieData.duration}
🗣️ Language: ${movieData.tag}
🎥 ${movieData.directors}

#${movieData.tag} #Movie #${movieData.year}`;

    const tags = [
      movieData.tag,
      'Movie',
      movieData.year,
      'Cinema',
      'Film'
    ];

    await bot.editMessageText(
      `📺 *Uploading to YouTube*\n\n🎬 ${movieData.title.substring(0, 40)}...\n\n${getProgressBar(5)} 5%\n\nPreparing upload...\n\n💡 Using resumable upload for reliability`,
      { chat_id: chatId, message_id: messageId, parse_mode: 'Markdown', reply_markup: item ? keyboards.cancelResume(item.taskId) : undefined }
    );

    const fileSize = fsSync.statSync(filePath).size;
    let uploadedBytes = 0;
    let lastPercent = 5;
    let lastUpdateTime = Date.now();

    console.log(`📤 Starting YouTube upload: ${title} (${(fileSize / (1024 * 1024)).toFixed(2)} MB)`);

    const res = await youtube.videos.insert({
      part: ['snippet', 'status'],
      requestBody: {
        snippet: {
          title: title,
          description: description,
          tags: tags,
          categoryId: '1' // Film & Animation
        },
        status: {
          privacyStatus: 'public',
          selfDeclaredMadeForKids: false
        }
      },
      media: {
        body: fsSync.createReadStream(filePath)
      }
    }, {
      // Resumable upload configuration
      onUploadProgress: async (evt) => {
        // Check for pause
        if (item) {
          while (item.paused && !item.cancelled) {
            const currentPercent = Math.floor((uploadedBytes / fileSize) * 100);
            await bot.editMessageText(
              `⏸️ *Upload Paused*\n\n🎬 ${movieData.title.substring(0, 40)}...\n\nUpload paused at ${currentPercent}%`,
              { chat_id: chatId, message_id: messageId, parse_mode: 'Markdown', reply_markup: keyboards.resumeTask(item.taskId) }
            ).catch(() => {});
            await new Promise(resolve => setTimeout(resolve, 2000));
          }
          
          if (item.cancelled) throw new Error('Task cancelled by user');
        }

        uploadedBytes = evt.bytesRead;
        const percent = Math.floor((uploadedBytes / fileSize) * 95) + 5; // 5% to 100%
        const uploadedMB = (uploadedBytes / (1024 * 1024)).toFixed(2);
        const totalMB = (fileSize / (1024 * 1024)).toFixed(2);
        
        // Calculate upload speed
        const now = Date.now();
        const timeDiff = (now - lastUpdateTime) / 1000; // seconds
        const speed = timeDiff > 0 ? (uploadedBytes / (1024 * 1024)) / ((now - lastUpdateTime) / 1000) : 0;
        
        // Update progress every 5% or every 30 seconds for very large files
        const shouldUpdate = percent >= lastPercent + 5 || percent >= 95 || (now - lastUpdateTime) > 30000;
        
        if (shouldUpdate) {
          lastPercent = percent;
          lastUpdateTime = now;
          
          try {
            await bot.editMessageText(
              `📺 *Uploading to YouTube*\n\n🎬 ${movieData.title.substring(0, 40)}...\n\n${getProgressBar(percent)} ${percent}%\n\n📊 ${uploadedMB} MB / ${totalMB} MB\n⚡ Speed: ${speed.toFixed(2)} MB/s\n\n🔄 Using resumable upload`,
              { chat_id: chatId, message_id: messageId, parse_mode: 'Markdown', reply_markup: item ? keyboards.cancelResume(item.taskId) : undefined }
            );
          } catch (err) {
            // Ignore telegram message edit errors
          }
        }
      }
    });

    if (item && item.cancelled) throw new Error('Task cancelled by user');

    await bot.editMessageText(
      `📺 *Uploading to YouTube*\n\n🎬 ${movieData.title.substring(0, 40)}...\n\n${getProgressBar(100)} 100%\n\n✅ Processing complete...`,
      { chat_id: chatId, message_id: messageId, parse_mode: 'Markdown' }
    );

    console.log('✅ YouTube upload successful!');
    console.log('📹 Video ID:', res.data.id);
    console.log('🔗 Video URL: https://youtu.be/' + res.data.id);

    return {
      success: true,
      id: res.data.id,
      url: `https://youtu.be/${res.data.id}`
    };

  } catch (error) {
    if (error.message === 'Task cancelled by user') throw error;
    
    console.error('❌ YouTube upload error:', error.message);
    console.error('Error details:', error.response?.data || error);
    
    // Check for specific YouTube API errors
    if (error.message.includes('invalid_grant') || error.message.includes('Token has been expired')) {
      console.log('🔐 YouTube token expired, need to re-authenticate');
      throw new Error('YouTube authentication expired. Please run /reauth command.');
    }
    
    if (error.message.includes('uploadLimitExceeded')) {
      throw new Error('YouTube upload limit exceeded. Please try again later.');
    }
    
    if (error.message.includes('videoTooLong')) {
      throw new Error('Video is too long. Your YouTube account must be verified to upload videos longer than 15 minutes. Use /verify for info.');
    }
    
    if (error.message.includes('fileTooLarge')) {
      throw new Error('File size exceeds YouTube limits (256 GB max).');
    }
    
    if (error.message.includes('quotaExceeded')) {
      throw new Error('YouTube API quota exceeded. Please try again tomorrow.');
    }
    
    throw new Error(`YouTube upload failed: ${error.message}`);
  }
}

// ============================================
// STARTUP & INITIALIZATION
// ============================================

async function initializeBot() {
  console.log('🚀 Initializing bot...');
  console.log('=====================================');
  
  await ensureDataDirectory();
  await loadProcessedMovies();
  await loadAnalytics();
  
  // Initialize YouTube auth
  try {
    youtubeAuth = await getYouTubeAuth();
    console.log('✅ YouTube authentication ready!');
    console.log('=====================================');
  } catch (error) {
    console.error('❌ YouTube authentication failed:', error.message);
    console.log('⚠️ Bot will start but YouTube uploads will fail until authenticated');
    console.log('Run the bot and use /reauth command to authenticate');
    console.log('=====================================');
  }
  
  console.log('✅ Bot ready!');
  console.log(`📊 ${processedMovies.size} movies in history`);
  console.log(`📈 ${analytics.totalMovies} total processed`);
  console.log(`👤 Admin: @${ADMIN_USERNAME}`);
  console.log('=====================================');
  console.log('🎬 YouTube Upload Limits:');
  console.log('   • Unverified: 15 minutes max');
  console.log('   • Verified: 12 hours max');
  console.log('   • Max file size: 256 GB');
  console.log('=====================================\n');
}

initializeBot().catch(error => {
  console.error('❌ Initialization error:', error);
  process.exit(1);
});

// ============================================
// ERROR HANDLING
// ============================================

bot.on('polling_error', (error) => console.error('Polling:', error.message));
process.on('uncaughtException', (error) => console.error('Exception:', error));
process.on('unhandledRejection', (error) => console.error('Rejection:', error));

// ============================================
// GRACEFUL SHUTDOWN
// ============================================

async function gracefulShutdown() {
  console.log('\n🛑 Shutting down...');
  
  if (currentProcessing) {
    currentProcessing.cancelled = true;
  }
  
  console.log('💾 Saving data...');
  await saveProcessedMovies();
  await saveAnalytics();
  
  console.log('✅ Data saved');
  console.log('👋 Goodbye!');
  
  bot.stopPolling();
  process.exit(0);
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

console.log('✅ Bot script loaded');
