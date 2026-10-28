# SAP SuccessFactors Data Extraction & Sync Pipeline

A robust, enterprise-grade Python pipeline designed to extract, transform, and load (ETL) data from **SAP SuccessFactors** (via OData APIs and SOAP SFAPI) into local file storage, MongoDB, and Azure Blob Storage.

---

## Architecture & Components

The pipeline is split into a multi-tier, modular execution model:

1. **`master_extract_collections_v3-stable.py` (Orchestrator)**:
* Manages concurrent task execution across multiple threads (`ThreadPoolExecutor`).


* Handles inclusion/exclusion filters for collections.


* Automatically detects and applies incremental loading strategies, tracks execution health, and aggregates log reports.




2. **`extract_collection_v3-stable.py` (Worker / Single Collection Pipeline)**:
* Handles authentication via **IdP** and **OAuth2**.


* Connects to **SAP SuccessFactors OData API** or **SOAP SFAPI** with built-in exponential backoff retry logic (`ExponentialBackoffSession`).


* Supports complex post-processing steps (field extraction, array filtering, value injection).


* Writes outputs locally (with Gzip compression options), upserts/inserts documents into **MongoDB**, and syncs backups to **Azure Storage** via `azcopy`.




3. **`collections_v4.json` (Configuration Schema)**:
* Defines all available entities, extraction queries, pagination settings, query expansions (`$expand`), delta filters, and MongoDB indexing rules.





---

## Prerequisites & Installation

* **Python 3.12+**


* **Python Dependencies**: Install via `pip` (or your requirements manager):
```bash
pip install requests urllib3 pymongo jsonpath-ng cryptography

```


* **Azure CLI / AzCopy** (Optional): Required if utilizing the `--azupload` feature.



---

## Configuration (`config.json`)

Ensure you have a `config.json` file in the same directory as the scripts. It should contain:

* **`sf`**: Connection endpoints and authentication keys for your SAP SuccessFactors environments.
* **`mongodb`**: Database connection URIs and credentials.


* **`azupload`**: Azure blob targets and `azcopy` execution settings.


* **`general`**: Log paths, HTTP retry settings, and optional **AES-GCM encryption secrets** paths.



---

## Usage Guide

### 1. Running the Master Orchestrator (Recommended)

To run all configured collections concurrently using 5 worker threads:

```bash
python master_extract_collections_v3-stable.py <environment_name> --parallel 5

```

#### Key Master Options:

* `--incremental`: Restricts execution strictly to collections that support incremental updates (`query_incremental`).


* `--full`: Runs only full load collections.


* `--force-full`: Forces full loads even on collections configured for incremental syncing.


* `--mongo-conn <name>`: Siphon extracted data directly into the specified MongoDB connection.


* `--azupload <target>`: Automatically push compressed JSON exports to Azure Blob Storage.


* `--compress` / `--gzip`: Compress local output files with Gzip.


* `--max-docs <N>`: Limit output to $N$ documents per collection (ideal for testing).


* `--dry-run`: Print the generated execution commands without running them.


* **Resource Filtering**: Pass specific collection names as arguments to run only those tasks, or prefix with `-` to exclude them (e.g., `User EmpJob -CompoundEmployee`).



---

### 2. Running a Single Collection Worker Directly

If you need to extract a single collection (e.g., `User` or `CompoundEmployee`) independently:

```bash
python extract_collection_v3-stable.py <environment_name> User --mongo-conn <conn_name> --incremental --compress

```

#### Key Worker Options:

* `--coll-config <filename>`: Override the default collection schema configuration file (defaults to `collections.json`).


* `--incremental`: Triggers incremental extraction based on the latest MongoDB watermark.


* `--output-dir <dir>`: Custom destination folder for local JSON file exports.


* `--coll-ts <iso_time>`: Override the collection run timestamp.



---

## Logging & Monitoring

* **Master Logs**: Stored under `logs/` with timestamps and unique batch identifiers (`-master-<batch_id>.log`).


* **Collection Logs**: Individual workers generate granular logs specified via `config.json` (defaulting to `logs/{coll}-{incr}-{ts}.log`). Both stdout and file handlers are equipped with formatting protections to handle long messages gracefully.