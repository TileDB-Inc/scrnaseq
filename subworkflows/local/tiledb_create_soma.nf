include { TILEDB_CREATE } from '../../modules/local/tiledb_create'

workflow TILEDB_CREATE_SOMA {
    take:
        s3_uri
        h5ad_file_ch
        rd

    main:
        TILEDB_CREATE (
            s3_uri,
            h5ad_file_ch,
            rd
        )
}
