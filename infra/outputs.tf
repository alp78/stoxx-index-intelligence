# --------------------------------------------------------------------------
# Terraform outputs — URLs and IPs for provisioned resources
# --------------------------------------------------------------------------

# the public URL of the Cloud Run dashboard service
output "dashboard_url" {
  value = google_cloud_run_v2_service.dashboard.uri  # resolved after resource creation
}

# the private VPC IP of the SQL Server VM
output "sql_vm_ip" {
  value = local.sql_ip
}

# the full Artifact Registry URL (used in docker push commands)
output "registry" {
  value = local.registry
}

# the public IP of the Airflow VM (for browser access to Airflow UI)
output "airflow_ip" {
  value = google_compute_instance.airflow.network_interface[0].access_config[0].nat_ip
}

# the Cloud Run pipeline job name (used in gcloud run jobs execute)
output "pipeline_job" {
  value = google_cloud_run_v2_job.pipeline.name
}

# the Cloud Run setup job name (used in gcloud run jobs execute)
output "setup_job" {
  value = google_cloud_run_v2_job.setup.name
}
