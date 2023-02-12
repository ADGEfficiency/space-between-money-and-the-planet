all: results

clean:
	rm -rf ./data/

clean-results:
	rm -rf ./data/results ./data/database.sqlite

./data/dataset.parquet:
	pip install -q -r ./requirements.txt
	python ./dataset.py
dataset: ./data/dataset.parquet

./data/database.sqlite: dataset
	rm -rf ./data/results
	python ./simulate.py
simulate: ./data/database.sqlite

results: simulate
	python results.py

pulls3:
	pip install -q awscli
	aws s3 sync s3://adgefficiency-public/space-between-2023/data ./data --no-sign-request

pushs3:
	pip install -q awscli
	aws s3 sync ./data s3://adgefficiency-public/space-between-2023/data --delete
