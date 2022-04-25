.PHONY: build

build:
	mkdir -p build
	rm build/app.zip || exit 0
	zip build/app.zip \
		*.py requirements.txt 

upload:
	gsutil cp data/dumps.db gs://sentineld-data/
	gsutil cp data/sheets_data.json gs://sentineld-data/


download:
	cp data/dumps.db data/dumps.db.bkp
	cp data/sheets_data.json data/sheets_data.json.bkp
	gsutil cp gs://sentineld-data/dumps.db data/
	gsutil cp gs://sheets_data.json data/
