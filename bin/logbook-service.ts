#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { LogbookServiceStack } from '../lib/logbook-service-stack';

const app = new cdk.App();

new LogbookServiceStack(app, 'LogbookServiceStack', {
  env: {
    account: '266798531919',
    region: 'us-west-2',
  },
  description: 'Logbook digitization service — API, PDF split, Gemini extraction',
});
