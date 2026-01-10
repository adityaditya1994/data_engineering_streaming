# Airflow Operations Handbook

A complete guide to deploying, managing, and troubleshooting Airflow for the Clickstream ETL project.

---

## Table of Contents

1. [Deployment Guide](#1-deployment-guide)
2. [Accessing Airflow](#2-accessing-airflow)
3. [Managing DAGs](#3-managing-dags)
4. [Service Management](#4-service-management)
5. [Monitoring & Logs](#5-monitoring--logs)
6. [Troubleshooting](#6-troubleshooting)
7. [Security Best Practices](#7-security-best-practices)
8. [Cost Optimization](#8-cost-optimization)

---

## 1. Deployment Guide

### Prerequisites

Before deploying, ensure you have:
- [ ] AWS Account with Free Tier eligibility
- [ ] EC2 Key Pair created in `eu-north-1` region
- [ ] Your public IP address (Google "what is my IP")

### Step-by-Step Deployment

#### Step 1: Deploy CloudFormation Stack

1. Go to **AWS Console → CloudFormation → Create Stack**
2. Upload template: `Cloudformations/ec2_airflow_emr.yaml`
3. Fill in parameters:

| Parameter | Value | Example |
|-----------|-------|---------|
| Stack name | Your choice | `airflow-prod` |
| KeyName | Your EC2 key pair | `my-key` |
| InstanceType | `t2.micro` | Free Tier eligible |
| AllowedIP | Your IP + /32 | `203.0.113.50/32` |
| VpcId | Select default VPC | `vpc-xxxxx` |
| SubnetId | Select public subnet | `subnet-xxxxx` |

4. Click **Next → Next → Create Stack**
5. Wait 5-7 minutes for deployment

#### Step 2: Verify Deployment

Check the **Outputs** tab for:
- `AirflowURL` - Your Airflow UI link
- `SSHCommand` - SSH command to connect
- `DAGsDirectory` - Where to upload DAGs

---

## 2. Accessing Airflow

### Web UI Access

```
URL:      http://<EC2_PUBLIC_IP>:8080
Username: admin
Password: admin
```

> ⚠️ **First Login**: Wait 3-5 minutes after stack creation for Airflow to initialize.

### SSH Access

```bash
# Connect to EC2
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>

# Switch to airflow user
sudo su - airflow

# Activate virtual environment
source /home/airflow/venv/bin/activate
export AIRFLOW_HOME=/home/airflow/airflow
```

### Finding Your EC2 Public IP

1. AWS Console → EC2 → Instances
2. Click on your Airflow instance
3. Copy **Public IPv4 address**

---

## 3. Managing DAGs

### Upload DAGs to EC2

```bash
# From your local machine
scp -i your-key.pem airflow/*.py ubuntu@<EC2_PUBLIC_IP>:/tmp/

# SSH into EC2
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>

# Move DAGs to correct location
sudo mv /tmp/*.py /home/airflow/airflow/dags/
sudo chown airflow:airflow /home/airflow/airflow/dags/*.py
```

### DAG Location on EC2

```
/home/airflow/airflow/dags/
```

### Refresh DAGs

DAGs are auto-detected every 5 minutes. To force refresh:

```bash
# SSH into EC2, then:
sudo systemctl restart airflow-scheduler
```

### Set Environment Variables for DAGs

```bash
# Edit the scheduler service
sudo nano /etc/systemd/system/airflow-scheduler.service

# Add under [Service] section:
Environment="EMR_CLUSTER_ID=j-XXXXXXXXXXXXX"
Environment="SCRIPT_BUCKET=your-scripts-bucket"
Environment="BRONZE_PATH=s3://your-bronze-bucket/clickstream/"
Environment="ICEBERG_DB=analytics"
Environment="EMR_LOG_URI=s3://your-logs-bucket/emr-logs/"

# Reload and restart
sudo systemctl daemon-reload
sudo systemctl restart airflow-scheduler
sudo systemctl restart airflow-webserver
```

---

## 4. Service Management

### Check Service Status

```bash
# Check webserver status
sudo systemctl status airflow-webserver

# Check scheduler status
sudo systemctl status airflow-scheduler
```

### Start/Stop/Restart Services

```bash
# Start services
sudo systemctl start airflow-webserver
sudo systemctl start airflow-scheduler

# Stop services
sudo systemctl stop airflow-webserver
sudo systemctl stop airflow-scheduler

# Restart services
sudo systemctl restart airflow-webserver
sudo systemctl restart airflow-scheduler
```

### Services Auto-Start on Reboot

Services are configured to start automatically. Verify with:

```bash
sudo systemctl is-enabled airflow-webserver
sudo systemctl is-enabled airflow-scheduler
```

---

## 5. Monitoring & Logs

### View Real-Time Logs

```bash
# Webserver logs
sudo journalctl -u airflow-webserver -f

# Scheduler logs
sudo journalctl -u airflow-scheduler -f

# Combined logs
sudo journalctl -u airflow-webserver -u airflow-scheduler -f
```

### View Historical Logs

```bash
# Last 100 lines of webserver logs
sudo journalctl -u airflow-webserver -n 100

# Logs from today
sudo journalctl -u airflow-webserver --since today

# Logs from specific time
sudo journalctl -u airflow-scheduler --since "2024-01-10 10:00" --until "2024-01-10 12:00"
```

### DAG Run Logs (in Airflow UI)

1. Click on DAG name
2. Click on a task instance
3. Click **Logs** button

### Task Logs Location

```
/home/airflow/airflow/logs/<dag_id>/<task_id>/<run_id>/
```

---

## 6. Troubleshooting

### Common Issues

#### Issue: Airflow UI Not Loading

```bash
# Check if webserver is running
sudo systemctl status airflow-webserver

# Check for port conflicts
sudo lsof -i :8080

# Restart webserver
sudo systemctl restart airflow-webserver

# Check logs for errors
sudo journalctl -u airflow-webserver -n 50
```

#### Issue: DAGs Not Appearing

```bash
# Check DAG file syntax
source /home/airflow/venv/bin/activate
export AIRFLOW_HOME=/home/airflow/airflow
python /home/airflow/airflow/dags/your_dag.py

# Check scheduler logs
sudo journalctl -u airflow-scheduler -n 50 | grep -i error

# Force DAG refresh
sudo systemctl restart airflow-scheduler
```

#### Issue: Task Failing

1. Check task logs in Airflow UI
2. Verify environment variables are set
3. Check IAM permissions for EMR/S3

```bash
# Test AWS credentials
aws sts get-caller-identity
aws s3 ls s3://your-bucket/
```

#### Issue: Out of Memory

```bash
# Check memory usage
free -h

# Check what's using memory
top -o %MEM

# If needed, increase swap
sudo fallocate -l 1G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

#### Issue: Database Locked (SQLite)

```bash
# Stop services
sudo systemctl stop airflow-webserver airflow-scheduler

# Check for stale processes
ps aux | grep airflow

# Kill if needed
sudo pkill -9 -f airflow

# Start services
sudo systemctl start airflow-webserver airflow-scheduler
```

### Reset Airflow Database

⚠️ **Warning**: This deletes all DAG history!

```bash
sudo systemctl stop airflow-webserver airflow-scheduler

sudo -u airflow bash -c '
  source /home/airflow/venv/bin/activate
  export AIRFLOW_HOME=/home/airflow/airflow
  rm -f $AIRFLOW_HOME/airflow.db
  airflow db migrate
  airflow users create --username admin --password admin \
    --firstname Admin --lastname User --role Admin --email admin@example.com
'

sudo systemctl start airflow-webserver airflow-scheduler
```

---

## 7. Security Best Practices

### Change Default Password

```bash
source /home/airflow/venv/bin/activate
export AIRFLOW_HOME=/home/airflow/airflow

# Change admin password
airflow users reset-password -u admin -p YOUR_NEW_STRONG_PASSWORD
```

### Restrict Access by IP

Update the CloudFormation `AllowedIP` parameter or modify the security group directly in AWS Console.

### Enable HTTPS (Production)

For production, set up an Application Load Balancer with SSL certificate or use nginx as a reverse proxy.

### Rotate Fernet Key

```bash
# Generate new key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Update airflow.cfg
nano /home/airflow/airflow/airflow.cfg
# Set: fernet_key = YOUR_NEW_KEY

# Restart services
sudo systemctl restart airflow-webserver airflow-scheduler
```

---

## 8. Cost Optimization

### Free Tier Usage (First 12 Months)

| Resource | Free Tier Limit | This Setup |
|----------|----------------|------------|
| t2.micro | 750 hrs/month | ✅ 720 hrs/month (24/7) |
| EBS Storage | 30 GB | ✅ 20 GB |
| Data Transfer | 100 GB/month | ✅ Minimal |

### Stop EC2 When Not in Use

```bash
# Stop from AWS Console or CLI
aws ec2 stop-instances --instance-ids i-xxxxxxxxxxxx --region eu-north-1

# Start when needed
aws ec2 start-instances --instance-ids i-xxxxxxxxxxxx --region eu-north-1
```

> ⚠️ Public IP changes when you stop/start. Check the new IP in AWS Console.

### Schedule Auto Stop/Start

Use AWS Instance Scheduler or create a simple cron-based Lambda to stop instances outside working hours.

---

## Quick Reference

### Important Paths

| Item | Path |
|------|------|
| Airflow Home | `/home/airflow/airflow/` |
| DAGs | `/home/airflow/airflow/dags/` |
| Logs | `/home/airflow/airflow/logs/` |
| Config | `/home/airflow/airflow/airflow.cfg` |
| Virtual Env | `/home/airflow/venv/` |
| Database | `/home/airflow/airflow/airflow.db` |

### Key Commands

```bash
# SSH to instance
ssh -i key.pem ubuntu@<IP>

# Upload DAGs
scp -i key.pem *.py ubuntu@<IP>:/tmp/

# Check status
sudo systemctl status airflow-webserver

# View logs
sudo journalctl -u airflow-scheduler -f

# Restart all
sudo systemctl restart airflow-webserver airflow-scheduler
```

### Useful Airflow CLI Commands

```bash
# Activate environment first
source /home/airflow/venv/bin/activate
export AIRFLOW_HOME=/home/airflow/airflow

# List DAGs
airflow dags list

# Trigger a DAG
airflow dags trigger clickstream_emr_kpis

# List tasks in a DAG
airflow tasks list clickstream_emr_kpis

# Test a specific task
airflow tasks test clickstream_emr_kpis create_emr_cluster 2024-01-01

# Pause/Unpause DAG
airflow dags pause clickstream_emr_kpis
airflow dags unpause clickstream_emr_kpis

# List users
airflow users list

# Create new user
airflow users create --username newuser --password pass123 \
  --firstname New --lastname User --role Viewer --email new@example.com
```

---

## Support

For issues with this setup:
1. Check the [Troubleshooting](#6-troubleshooting) section
2. Review logs with `sudo journalctl -u airflow-webserver -n 100`
3. Refer to [Apache Airflow Documentation](https://airflow.apache.org/docs/)
