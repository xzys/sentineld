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
	for f in data/{dumps.db,sheets_data.json}; do \
		cp "$$f" "$$f.bkp"; \
		gsutil cp gs://sentineld-data/`basename $$f` data/; \
	done;
