# Railway Deployment Setup for jarvizbot-backend

## Environment Variables Configuration

Set these environment variables in your Railway project:

### PostgreSQL Connection (Supabase Pooler)

1. Go to your Railway project dashboard
2. Click on your service
3. Go to **"Variables"** tab
4. Add the following environment variables:

```
PGHOST=aws-1-ap-southeast-2.pooler.supabase.com
PGPORT=5432
PGDATABASE=postgres
PGUSER=postgres.yzkbleuhvrhjabrpwlow
PGPASSWORD=K14gKTz1NyHcrsFx
PGSSLMODE=require
```

### Bot Token (Optional - if not using credentials.txt)

```
BOT_TOKEN=your_telegram_bot_token_here
```

## Steps to Configure:

1. **Login to Railway**: https://railway.app
2. **Select your project** → **Select your service** (jarvizbot-backend)
3. **Click "Variables"** tab
4. **Click "New Variable"** for each variable above
5. **Enter the variable name** (e.g., `PGHOST`)
6. **Enter the variable value** (e.g., `aws-1-ap-southeast-2.pooler.supabase.com`)
7. **Click "Add"**
8. **Repeat for all variables**
9. **Redeploy** your service (Railway will automatically redeploy when you add variables)

## Verification:

After setting the variables and redeploying, check your Railway logs to ensure:
- Database connection is successful
- Bot is connecting to Supabase
- No connection errors

## Notes:

- Railway will automatically redeploy when you add/update environment variables
- Make sure `PGPASSWORD` is set correctly (it's case-sensitive)
- The connection pooler should work reliably on Railway
- Your bot will connect to the same Supabase database as your local environment and Streamlit dashboards

