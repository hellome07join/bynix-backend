#!/bin/bash
# Bynix Backend - Build and Push to AWS ECR

echo "🔧 Building Docker Image..."

# Login to ECR
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 695650812447.dkr.ecr.ap-south-1.amazonaws.com

# Build image
docker build -t bynix-backend .

# Tag image
docker tag bynix-backend:latest 695650812447.dkr.ecr.ap-south-1.amazonaws.com/bynix-backend:latest

# Push to ECR
docker push 695650812447.dkr.ecr.ap-south-1.amazonaws.com/bynix-backend:latest

echo "✅ Image pushed to ECR!"
