#!/bin/bash


echo 'Set environment'
export REGION=us-central1
export ZONE=us-central1-f
export PROJECT_ID=$1
export COMPOSER_ENVIRONMENT_NAME=devfest_demo_composer

export CLOUD_COMPOSER_SERVICE_ACCOUNT=devfest_demo_composer_sa

export COMPOSER_IMAGE=composer-1.12.2-airflow-1.10.10

echo 'Create service account'
gcloud iam service-accounts create $CLOUD_COMPOSER_SERVICE_ACCOUNT \
	  --display-name=$CLOUD_COMPOSER_SERVICE_ACCOUNT \
	    --project=$PROJECT_ID

ACCOUNT_ROLE=('composer.worker' 'composer.environmentAndStorageObjectViewer')
for role in "${ACCOUNT_ROLE[@]}"
do
	gcloud projects add-iam-policy-binding $PROJECT_ID \
		--member serviceAccount:$CLOUD_COMPOSER_SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com \
		--role=roles/$role
done

echo 'Create Cloud Composer'
gcloud composer environments create $COMPOSER_ENVIRONMENT_NAME \
	--location $REGION \
	--zone $ZONE \
	--disk-size=20GB \
	--machine-type=n1-standard-1 \
	--node-count=3 \
	--python-version=3 \
	--image-version=$COMPOSER_IMAGE \
	--service-account=$CLOUD_COMPOSER_SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com \
	--project=$PROJECT_ID


