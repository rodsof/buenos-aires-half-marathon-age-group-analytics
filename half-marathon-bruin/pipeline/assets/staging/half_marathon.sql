/* @bruin

name: staging.half_marathon
type: bq.sql

materialization:
  type: table
  strategy: time_interval
  partition_by: edition_date
  cluster_by:
    - gender
    - age_group
  incremental_key: edition_date
  time_granularity: date

depends:
  - setup.ensure_staging_dataset
  - ingestion.half_marathon_raw

columns:
  - name: edition_date
    type: date
    description: Race edition year
    checks:
      - name: not_null
  - name: edition
    type: string
    description: Race edition year
    checks:
      - name: not_null
  - name: position
    type: integer
    description: Runner's finishing position
    checks:
      - name: not_null
  - name: bib_number
    type: string
    description: Runner's bib number
    checks:
      - name: not_null
  - name: name
    type: string
    description: Runner's name
    checks:
      - name: not_null
  - name: age_group
    type: string
    description: Runner's age group
  - name: gender
    type: string
    description: Runner's gender
  - name: gun_time
    type: string
    description: Runner's finish time (gun time) in HH:MM:SS format
  - name: chip_time
    type: string
    description: Runner's finish time (chip time) in HH:MM:SS format
  - name: chip_time_hours
    type: float
    description: Runner's finish time (chip time) in hours
    checks:
      - name: not_null

@bruin */

SELECT
    CAST(edition AS STRING) AS edition,
    DATE(SAFE_CAST(edition AS INT64), 1, 1) AS edition_date,
    CAST(position AS INT64) AS position,
    CAST(bib_number AS STRING) AS bib_number,
    CONCAT(CONCAT('_user_', bib_number), CAST(edition AS STRING)) AS anonymized_user,
    CAST(gender AS STRING) AS gender,
    CAST(age_group AS STRING) AS age_group,
    CAST(gun_time AS STRING) AS gun_time,
    CAST(chip_time AS STRING) AS chip_time,
    (
        CAST(SPLIT(TRIM(chip_time), ':')[OFFSET(0)] AS FLOAT64)
        + CAST(SPLIT(TRIM(chip_time), ':')[OFFSET(1)] AS FLOAT64) / 60.0
        + CAST(SPLIT(TRIM(chip_time), ':')[OFFSET(2)] AS FLOAT64) / 3600.0
    ) AS chip_time_hours
FROM `ingestion.half_marathon_raw`
WHERE DATE(SAFE_CAST(edition AS INT64), 1, 1) >= '{{ start_date }}'
  AND DATE(SAFE_CAST(edition AS INT64), 1, 1) < '{{ end_date }}'
  AND edition IS NOT NULL
  AND chip_time IS NOT NULL
  AND ARRAY_LENGTH(SPLIT(TRIM(chip_time), ':')) = 3
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY edition, bib_number
    ORDER BY CAST(position AS INT64)
) = 1
