process TILEDB_REGISTER_SOMA {
    tag "$s3_url"
    label 'process_medium'


    conda "conda-forge::tiledb tiledb::tiledbsoma-py"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/tiledb-soma:latest' :
        'dockerhub/tiledbsoma-py:latest' }"

    input:
    val s3_url
    val h5ad_files

    output:
    path "versions.yml", emit: versions
    path "rd.json", emit: rd

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    #!/usr/bin/env python3
    import scanpy as sc
    import tiledbsoma
    import tiledbsoma.io
    import tiledbsoma.logging
    import json
    h5ad_py_list = [path.strip() for path in "${h5ad_files}".strip("[]").split(",") if "filtered" in path]

    first_h5ad = h5ad_py_list[0]
    # ideally this would be empty, just use the file as a template
    first = tiledbsoma.io.from_h5ad(experiment_uri="${s3_url}", input_path=first_h5ad, measurement_name="RNA")

    rd = tiledbsoma.io.register_h5ads(
        experiment_uri="${s3_url}",
        h5ad_file_names=h5ad_py_list,
        measurement_name="RNA",
        obs_field_name="obs_id",
        var_field_name="var_id",
    )

    #convert to json string
    rd_jsons = rd.to_json()

    #save to json
    with open("rd.json", "w") as f:
        f.write(rd_jsons)
        
    with open("versions.yml", "w") as f:
        f.write("'${task.process}':\\n")
        f.write(f"    tiledbsoma: {tiledbsoma.__version__}\\n")
        f.write(f"    scanpy: {sc.__version__}\\n")
    """
}
