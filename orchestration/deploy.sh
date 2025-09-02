#!/bin/bash

# AWS orchestration deployment script

set -e

echo "Starting orchestration pipeline deployment"
echo "==========================================="

# Configuration
PROJECT_NAME="restaurant-pipeline"
REGION="eu-west-1"
STACK_NAME="${PROJECT_NAME}-orchestration"

# Prerequisites check
echo "Checking prerequisites..."

if ! command -v aws &> /dev/null; then
    echo "AWS CLI not found. Install it with: pip install awscli"
    exit 1
fi

if ! aws sts get-caller-identity &> /dev/null; then
    echo "AWS CLI is not configured. Run: aws configure"
    exit 1
fi

echo "AWS CLI is configured"

# Prompt for parameters
read -p "Google Places API Key: " GOOGLE_API_KEY
read -p "Notification email: " NOTIFICATION_EMAIL

if [ -z "$GOOGLE_API_KEY" ] || [ -z "$NOTIFICATION_EMAIL" ]; then
    echo "Missing parameters"
    exit 1
fi

# Create Lambda package
echo "Creating Lambda package..."

# Create a temporary directory
TEMP_DIR=$(mktemp -d)
echo "Temporary directory: $TEMP_DIR"

# Copy Lambda files
cp lambda_orchestrator.py "$TEMP_DIR/"
cp lambda_collector.py "$TEMP_DIR/"

# Copy existing collector
cp ../src/google_places_collector.py "$TEMP_DIR/"

# Create requirements.txt for Lambda
cat > "$TEMP_DIR/requirements.txt" << EOF
boto3==1.26.137
requests==2.31.0
pandas==2.0.3
python-dotenv==1.0.0
duckdb==0.8.1
EOF

# Install dependencies into the package
echo "Installing dependencies..."
cd "$TEMP_DIR"
pip install -r requirements.txt -t .

# Create ZIP file
echo "Creating ZIP package..."
zip -r ../lambda-package.zip . -x "*.pyc" "__pycache__/*"
cd ..

# Upload the package to S3 (required for large packages)
echo "Uploading package to S3..."

# Create a temporary bucket if needed
BUCKET_NAME="${PROJECT_NAME}-deploy-$(date +%s)"
aws s3 mb "s3://$BUCKET_NAME" --region "$REGION"

aws s3 cp lambda-package.zip "s3://$BUCKET_NAME/"

# Deploy the CloudFormation stack
echo "Deploying CloudFormation stack..."

aws cloudformation deploy \
    --template-file cloudformation_template.yaml \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --parameter-overrides \
        ProjectName="$PROJECT_NAME" \
        GooglePlacesApiKey="$GOOGLE_API_KEY" \
        NotificationEmail="$NOTIFICATION_EMAIL" \
    --capabilities CAPABILITY_NAMED_IAM \
    --no-fail-on-empty-changeset

# Update Lambda function code
echo "Updating Lambda code..."

# Get function names from the stack
ORCHESTRATOR_FUNCTION=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`OrchestratorFunctionArn`].OutputValue' \
    --output text | cut -d':' -f7)

COLLECTOR_FUNCTION=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`CollectorFunctionArn`].OutputValue' \
    --output text | cut -d':' -f7)

# Update functions with the real code
aws lambda update-function-code \
    --function-name "$ORCHESTRATOR_FUNCTION" \
    --s3-bucket "$BUCKET_NAME" \
    --s3-key "lambda-package.zip" \
    --region "$REGION"

aws lambda update-function-code \
    --function-name "$COLLECTOR_FUNCTION" \
    --s3-bucket "$BUCKET_NAME" \
    --s3-key "lambda-package.zip" \
    --region "$REGION"

# Cleanup
echo "Cleaning up..."
rm -rf "$TEMP_DIR"
rm lambda-package.zip
aws s3 rm "s3://$BUCKET_NAME/lambda-package.zip"
aws s3 rb "s3://$BUCKET_NAME"

# Deployment information
echo "Deployment details:"
echo "====================="

aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' \
    --output table

echo ""
echo "Deployment completed successfully."
echo ""
echo "Next steps:"
echo "1. Check your email to confirm the SNS subscription"
echo "2. Manually test the pipeline:"
echo "   aws lambda invoke --function-name $ORCHESTRATOR_FUNCTION response.json"
echo "3. The pipeline will run automatically every day at 8 AM UTC"
echo "4. Use the CloudWatch dashboard for monitoring" 