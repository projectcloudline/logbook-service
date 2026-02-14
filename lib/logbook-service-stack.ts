import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as acm from 'aws-cdk-lib/aws-certificatemanager';
import * as route53 from 'aws-cdk-lib/aws-route53';
import * as route53Targets from 'aws-cdk-lib/aws-route53-targets';
import * as lambdago from '@aws-cdk/aws-lambda-go-alpha';
import { Construct } from 'constructs';
import * as path from 'path';

export class LogbookServiceStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ─── VPC (existing Forge VPC with fck-nat) ─────────────────
    const vpc = ec2.Vpc.fromLookup(this, 'ForgeVpc', {
      vpcId: 'vpc-0b9528238d153559f',
    });

    const lambdaSg = new ec2.SecurityGroup(this, 'LambdaSecurityGroup', {
      vpc,
      description: 'Logbook Lambda functions',
      allowAllOutbound: true,
    });

    const lambdaVpcConfig = {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [lambdaSg],
    };

    // ─── Secrets ───────────────────────────────────────────────
    const dbSecret = secretsmanager.Secret.fromSecretCompleteArn(this, 'DbSecret',
      'arn:aws:secretsmanager:us-west-2:266798531919:secret:dev/forge/db-credentials-QtTX2H'
    );
    const appSecrets = secretsmanager.Secret.fromSecretNameV2(this, 'AppSecrets',
      'dev/forge/app/secrets'
    );
    const faaRegistryApiKey = secretsmanager.Secret.fromSecretNameV2(this, 'FaaRegistryApiKey',
      'staging/cloudline/faa-registry-api/api-key'
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

    // ─── API Lambda (Go) ────────────────────────────────────────
    const apiFunction = new lambdago.GoFunction(this, 'ApiFunction', {
      functionName: 'logbook-api',
      entry: path.join(__dirname, '..', 'lambdas', 'api'),
      runtime: lambda.Runtime.PROVIDED_AL2023,
      architecture: lambda.Architecture.ARM_64,
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      environment: {
        ...sharedEnv,
        FAA_REGISTRY_URL: 'https://faa-registry.staging.cloudline.aero',
        FAA_REGISTRY_SECRET_ARN: faaRegistryApiKey.secretArn,
      },
      ...lambdaVpcConfig,
    });

    // ─── Split Lambda (Go) ──────────────────────────────────────
    const splitFunction = new lambdago.GoFunction(this, 'SplitFunction', {
      functionName: 'logbook-split',
      entry: path.join(__dirname, '..', 'lambdas', 'split'),
      runtime: lambda.Runtime.PROVIDED_AL2023,
      architecture: lambda.Architecture.ARM_64,
      timeout: cdk.Duration.minutes(5),
      memorySize: 1024,
      ephemeralStorageSize: cdk.Size.mebibytes(1024),
      environment: sharedEnv,
      bundling: {
        commandHooks: {
          beforeBundling: (_inputDir: string, _outputDir: string) => [],
          afterBundling: (inputDir: string, outputDir: string) => [
            `cp ${inputDir}/bin/mutool-arm64 ${outputDir}/bin/mutool-arm64 2>/dev/null || true`,
          ],
        },
      },
      ...lambdaVpcConfig,
    });

    // ─── Analyze Lambda (Go) ────────────────────────────────────
    const analyzeFunction = new lambdago.GoFunction(this, 'AnalyzeFunction', {
      functionName: 'logbook-analyze',
      entry: path.join(__dirname, '..', 'lambdas', 'analyze'),
      runtime: lambda.Runtime.PROVIDED_AL2023,
      architecture: lambda.Architecture.ARM_64,
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
      environment: sharedEnv,
      reservedConcurrentExecutions: 5, // rate-limit Gemini calls
      ...lambdaVpcConfig,
    });

    // ─── Permissions ───────────────────────────────────────────
    bucket.grantReadWrite(apiFunction);
    bucket.grantReadWrite(splitFunction);
    bucket.grantReadWrite(analyzeFunction);

    dbSecret.grantRead(apiFunction);
    dbSecret.grantRead(splitFunction);
    dbSecret.grantRead(analyzeFunction);
    appSecrets.grantRead(analyzeFunction);
    appSecrets.grantRead(apiFunction); // for RAG endpoint
    faaRegistryApiKey.grantRead(apiFunction);

    analyzeQueue.grantSendMessages(splitFunction);
    analyzeQueue.grantConsumeMessages(analyzeFunction);

    // ─── Event Sources ─────────────────────────────────────────
    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_PUT,
      new s3n.LambdaDestination(splitFunction),
      { prefix: 'uploads/' }
    );

    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_PUT,
      new s3n.LambdaDestination(splitFunction),
      { prefix: 'pages/' }
    );

    analyzeFunction.addEventSource(
      new lambdaEventSources.SqsEventSource(analyzeQueue, {
        batchSize: 1,
      })
    );

    // ─── Custom Domain ────────────────────────────────────────
    const domainName = 'logbooks.cloudline.aero';
    const hostedZone = route53.HostedZone.fromHostedZoneAttributes(this, 'LogbooksZone', {
      hostedZoneId: 'Z01347853MP9W5UL29YNK',
      zoneName: domainName,
    });

    const certificate = new acm.Certificate(this, 'LogbooksCert', {
      domainName,
      validation: acm.CertificateValidation.fromDns(hostedZone),
    });

    // ─── API Gateway ───────────────────────────────────────────
    const api = new apigateway.RestApi(this, 'LogbookApi', {
      restApiName: 'Logbook Service',
      description: 'Aircraft logbook digitization API',
      apiKeySourceType: apigateway.ApiKeySourceType.HEADER,
      deployOptions: { stageName: 'v1' },
      endpointTypes: [apigateway.EndpointType.REGIONAL],
      domainName: {
        domainName,
        certificate,
        endpointType: apigateway.EndpointType.REGIONAL,
      },
    });

    const lambdaIntegration = new apigateway.LambdaIntegration(apiFunction);

    // POST /uploads
    const uploads = api.root.addResource('uploads');
    uploads.addMethod('POST', lambdaIntegration, { apiKeyRequired: true });

    // /uploads/{id}/*
    const uploadById = uploads.addResource('{id}');

    // GET /uploads/{id}/status
    const status = uploadById.addResource('status');
    status.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    // GET /uploads/{id}/pages/{pageNumber}/image
    const uploadPages = uploadById.addResource('pages');
    const uploadPageByNumber = uploadPages.addResource('{pageNumber}');
    const pageImage = uploadPageByNumber.addResource('image');
    pageImage.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    // /aircraft/{tailNumber}/*
    const aircraft = api.root.addResource('aircraft');
    const byTail = aircraft.addResource('{tailNumber}');
    const tailUploads = byTail.addResource('uploads');
    tailUploads.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    const summary = byTail.addResource('summary');
    summary.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    const query = byTail.addResource('query');
    query.addMethod('POST', lambdaIntegration, { apiKeyRequired: true });

    const entries = byTail.addResource('entries');
    entries.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });

    const entryById = entries.addResource('{entryId}');
    entryById.addMethod('GET', lambdaIntegration, { apiKeyRequired: true });
    entryById.addMethod('PATCH', lambdaIntegration, { apiKeyRequired: true });

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

    // ─── Lambda Warmer ─────────────────────────────────────────
    new events.Rule(this, 'ApiWarmerRule', {
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      targets: [new eventsTargets.LambdaFunction(apiFunction, {
        event: events.RuleTargetInput.fromObject({ source: 'logbook.warmer' }),
      })],
    });

    // ─── DNS Record ────────────────────────────────────────────
    new route53.ARecord(this, 'LogbooksARecord', {
      zone: hostedZone,
      target: route53.RecordTarget.fromAlias(new route53Targets.ApiGateway(api)),
    });

    // ─── Outputs ───────────────────────────────────────────────
    new cdk.CfnOutput(this, 'CustomDomainUrl', { value: `https://${domainName}/` });
    new cdk.CfnOutput(this, 'ApiUrl', { value: api.url });
    new cdk.CfnOutput(this, 'BucketName', { value: bucket.bucketName });
    new cdk.CfnOutput(this, 'QueueUrl', { value: analyzeQueue.queueUrl });
    new cdk.CfnOutput(this, 'ApiKeyId', {
      value: apiKey.keyId,
      description: 'Retrieve with: aws apigateway get-api-key --api-key <id> --include-value',
    });
  }
}
