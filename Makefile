.PHONY: build

REGION = us-east1
FUNC_NAME = monitor-web


get-current-version:
	@echo "current version: `gcloud beta functions describe $(FUNC_NAME) --region=$(REGION) | ag versionId | cut -d' ' -f2`"


build:
	mkdir -p build
	rm build/app.zip || exit 0
	zip build/app.zip \
		*.py requirements.txt 


build-push: get-current-version build
	gsutil cp build/app.zip gs://sentineld-data/
	gcloud beta functions deploy $(FUNC_NAME) \
		--region=$(REGION) \
		--source=gs://sentineld-data/app.zip


upload:
	gsutil cp data/dumps.db gs://sentineld-data/
	gsutil cp data/sheets_data.json gs://sentineld-data/


download:
	for f in data/{dumps.db,sheets_data.json}; do \
		cp "$$f" "$$f.bkp"; \
		gsutil cp gs://sentineld-data/`basename $$f` data/; \
	done;
