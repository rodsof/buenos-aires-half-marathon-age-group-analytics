/* @bruin

name: ingestion.half_marathon_raw
type: bq.sql
connection: gcp

materialization:
  type: table
  strategy: create+replace

depends:
  - ingestion.half_marathon_gcs

secrets:
  - key: gcp
    inject_as: gcp

columns:
  - name: edition
    type: STRING
  - name: position
    type: INTEGER
  - name: bib_number
    type: STRING
  - name: name
    type: STRING
  - name: gender
    type: STRING
  - name: age_group
    type: STRING
  - name: gun_time
    type: STRING
  - name: chip_time
    type: STRING
  - name: chip_time_hours
    type: FLOAT

@bruin */

SELECT
  CAST(edition AS STRING) AS edition,
  SAFE_CAST(position AS INT64) AS position,
  CAST(bib_number AS STRING) AS bib_number,
  CAST(name AS STRING) AS name,
  CAST(gender AS STRING) AS gender,
  CAST(age_group AS STRING) AS age_group,
  CAST(gun_time AS STRING) AS gun_time,
  CAST(chip_time AS STRING) AS chip_time,
  SAFE_CAST(chip_time_hours AS FLOAT64) AS chip_time_hours
FROM `moonlit-state-486723-r7.dezoomcamp2026_project.half_marathon_external`
