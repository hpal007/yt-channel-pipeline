
 # Changelog
 All notable changes to this project will be documented in this file.

 The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

 ## [Unreleased]

## [0.0.3] - 2025-06-16
### Added 
 - Added `save_to_database` dag to save extracted files to database in SQLite db.
 - `db_utils` file holds all the connection and table creation details of the database. 
 
### Changed 
 - `drop_location` with `LOCAL_DATA_DIR`

## [0.0.2] - 2025-06-11
### Added
 - Integrated `black` code formatter to ensure consistent code style across the project.
 - Added `remove_local_folder` task in `upload_to_azure_storage_dag` to remove local files. 

### Changed 
 - `upload_to_azure_storage_dag` to upload multiple files on azure storage.
 -  Changed name of `upload_to_azure_storage_dag` to `save_and_delete`.
 
### Fixed
 -  `trigger_upload_to_azure_storage_dag` by removing hardcoded `channel_id`.

## [0.0.1] - 2025-06-11
### Added 
 - Initializes `CHANGELOG.md` to track project changes. 
 - Added `apache-airflow-providers-microsoft-azure` lib to work with azure storage.  
 - `others` folder and moved not used code files to this folder. Files moved `api_data_processing.py`. 
 - New dag `upload_to_azure_storage_dag` was created, to store the files from airflow to azure storage. New connection was created from `aiflow ui >> admin >> connection`, with connection type as `wasb` and with connection string from Azure.  
 - Task `trigger_upload_to_azure_storage_dag` was added, to trigger the dag `upload_to_azure_storage_dag`.

### Removed
 - `api_data_processing.py` dag was removed from dag folder. 

### Fixed
 - Task `extract_and_save_comments_info` was failing in DAG, when there is not comment on video, this was handaled in `yt-comment.py` by adding check before writing to storage. 
