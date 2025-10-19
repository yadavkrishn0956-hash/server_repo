# Dataset Bazar - Backend API

FastAPI backend for the Dataset Bazar marketplace.

## 🚀 Deploy to Vercel

1. **Push to GitHub**
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin YOUR_GITHUB_REPO_URL
   git push -u origin main
   ```

2. **Deploy on Vercel**
   - Go to https://vercel.com/new
   - Import your GitHub repository
   - Vercel will auto-detect Python
   - Click **Deploy**

3. **Add Environment Variables** (Optional)
   - Go to Project Settings → Environment Variables
   - Add: `STORAGE_PATH` = `/tmp/storage`
   - Add: `CORS_ORIGINS` = `YOUR_FRONTEND_URL`

## 🛠️ Local Development

```bash
pip install -r requirements.txt
python seed_data.py  # Initialize sample data
uvicorn main:app --reload
```

Open http://localhost:8000/docs for API documentation.

## 📚 API Endpoints

- `GET /health` - Health check
- `GET /api/datasets` - List datasets
- `POST /api/generate` - Generate dataset
- `POST /api/upload` - Upload dataset
- `GET /api/metadata/{cid}` - Get dataset details
