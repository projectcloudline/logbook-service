import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';
import * as path from 'path';

export class LogbookServiceStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ─── Secrets ───────────────────────────────────────────────
    const dbSecret = secretsmanager.Secret.fromSecretCompleteArn(this, 'DbSecret',
      'arn:aws:secretsmanager:us-west-2:266798531919:secret:dev/forge/db-credentials-QtTX2H'
    );
    const appSecrets = secretsmanager.Secret.fromSecretNameV2(this, 'AppSecrets',
      'dev/forge/app/secrets'
    );

    // ─── S3 Bucket ─────────────────────────────────────────────
    const bucket = new s3.Bucket(this, 'LogbookBucket', {
      bucketName: `cloudline-logbook-${this.account}`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      cors: [{
        allowedMethods: [s3.HttpMethods.PUT, s3.HttpMethods.GET],
        allowedOrigins: ['*'],
        allowedHeaders: ['*'],
      }],
    });

    // ─── SQS Queue ─────────────────────────────────────────────
    const dlq = new sqs.Queue(this, 'AnalyzeDLQ', {
      queueName: 'logbook-analyze-dlq',
      retentionPeriod: cdk.Duration.days(14),
    });

    const analyzeQueue = new sqs.Queue(this, 'AnalyzeQueue', {
      queueName: 'logbook-analyze-queue',
      visibilityTimeout: cdk.Duration.minutes(10),
      deadLetterQueue: { queue: dlq, maxReceiveCount: 3 },
    });

    // ─── Shared env vars ───────────────────────────────────────
    const sharedEnv: Record<string, string> = {
      BUCKET_NAME: bucket.bucketName,
      DB_SECRET_ARN: dbSecret.secretArn,
      GEMINI_SECRET_ARN: appSecrets.secretArn,
      ANALYZE_QUEUE_URL: analyzeQueue.queueUrl,
    };

    // ─── Lambda Layer (shared code) ────────────────────────────
    // Shared Python code is bundled via PYTHONPATH in each Lambda.
    // For pymupdf and google-genai, use Docker-bundled Lambdas.

    // ─── API Lambda ────────────────────────────────────────────
    const apiFunction = new lambda.Function(this, 'ApiFunction', {
      functionName: 'logbook-api',
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'api'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && ' +
            'cp -r . /asset-output/ && ' +
            'cp -r /asset-input/shared /asset-output/shared'
          ],
          volumes: [{
            hostPath: path.join(__dirname, '..', 'lambda', 'shared'),
            containerPath: '/asset-input/shared',
          }],
        },
      }),
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      environment: sharedEnv,
    });

    // ─── Split Lambda ──────────────────────────────────────────
    const splitFunction = new lambda.Function(this, 'SplitFunction', {
      functionName: 'logbook-split',
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'split'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && ' +
            'cp -r . /asset-output/ && ' +
            'cp -r /asset-input/shared /asset-output/shared'
          ],
          volumes: [{
            hostPath: path.join(__dirname, '..', 'lambda', 'shared'),
            containerPath: '/asset-input/shared',
          }],
        },
      }),
      timeout: cdk.Duration.minutes(5),
      memorySize: 1024,
      ephemeralStorageSize: cdk.Size.mebibytes(1024),
      environment: sharedEnv,
    });

    // ─── Analyze Lambda ────────────────────────────────────────
    const analyzeFunction = new lambda.Function(this, 'AnalyzeFunction', {
      functionName: 'logbook-analyze',
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'analyze'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && ' +
            'cp -r . /asset-output/ && ' +
            'cp -r /asset-input/shared /asset-output/shared'
          ],
          volumes: [{
            hostPath: path.join(__dirname, '..', 'lambda', 'shared'),
            containerPath: '/asset-input/shared',
          }],
        },
      }),
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
      environment: sharedEnv,
      reservedConcurrentExecutions: 5, // rate-limit Gemini calls
    });

    // ─── Permissions ───────────────────────────────────────────
    bucket.grantReadWrite(apiFunction);
    bucket.grantReadWrite(splitFunction);
    bucket.grantRead(analyzeFunction);

    dbSecret.grantRead(apiFunction);
    dbSecret.grantRead(splitFunction);
    dbSecret.grantRead(analyzeFunction);
    appSecrets.grantRead(analyzeFunction);
    appSecrets.grantRead(apiFunction); // for RAG endpoint

    analyzeQueue.grantSendMessages(splitFunction);
    analyzeQueue.grantConsumeMessages(analyzeFunction);

    // ─── Event Sources ─────────────────────────────────────────
    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_PUT,
      new s3n.LambdaDestination(splitFunction),
      { prefix: 'uploads/' }
    );

    analyzeFunction.addEventSource(
      new lambdaEventSources.SqsEventSource(analyzeQueue, {
        batchSize: 1,
      })
    );

    // ─── API Gateway ───────────────────────────────────────────
    const api = new apigateway.RestApi(this, 'LogbookApi', {
      restApiName: 'Logbook Service',
      description: 'Aircraft logbook digitization API',
      apiKeySourceType: apigateway.ApiKeySourceType.HEADER,
      deployOptions: { stageName: 'v1' },
    });

    const lambdaIntegration = new apigateway.LambdaIntegration(apiFunction);

    // POST /logbooks/upload
    const logbooks = api.root.addResource('logbooks');
    const upload = logbooks.addResource('upload');
    upload.addMethod('POST', lambdaIntegration, { apiKeyRequired: true });

    // GET /logbooks/{id}/status
    const logbookById = logbooks.addResource('{id}');
    const status = logbookById.addResource('status');
    status.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    // /aircraft/{tailNumber}/*
    const aircraft = api.root.addResource('aircraft');
    const byTail = aircraft.addResource('{tailNumber}');
    const tailLogbooks = byTail.addResource('logbooks');
    tailLogbooks.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    const summary = byTail.addResource('summary');
    summary.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    const query = byTail.addResource('query');
    query.addMethod('POST', lambdaIntegration, { apiKeyRequired: true });

    const entries = byTail.addResource('entries');
    entries.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    const entryById = entries.addResource('{entryId}');
    entryById.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    const inspections = byTail.addResource('inspections');
    inspections.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    const ads = byTail.addResource('ads');
    ads.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    const parts = byTail.addResource('parts');
    parts.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    // ─── API Key & Usage Plan ──────────────────────────────────
    const apiKey = api.addApiKey('LogbookApiKey', {
      apiKeyName: 'logbook-service-key',
    });

    const usagePlan = api.addUsagePlan('LogbookUsagePlan', {
      name: 'logbook-standard',
      throttle: { rateLimit: 10, burstLimit: 20 },
    });
    usagePlan.addApiKey(apiKey);
    usagePlan.addApiStage({ stage: api.deploymentStage });

    // ─── Outputs ───────────────────────────────────────────────
    new cdk.CfnOutput(this, 'ApiUrl', { value: api.url });
    new cdk.CfnOutput(this, 'BucketName', { value: bucket.bucketName });
    new cdk.CfnOutput(this, 'QueueUrl', { value: analyzeQueue.queueUrl });
    new cdk.CfnOutput(this, 'ApiKeyId', {
      value: apiKey.keyId,
      description: 'Retrieve with: aws apigateway get-api-key --api-key <id> --include-value',
    });
  }
}
