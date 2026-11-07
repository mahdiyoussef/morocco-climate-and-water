select dataset, year, path, bytes, sha256 from {{ source('silver', 'source_audit') }}
