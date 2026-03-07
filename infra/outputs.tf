# --------------------------------------------------------------------------
# Terraform outputs — URLs and IPs for provisioned resources
# --------------------------------------------------------------------------

output "dashboard_url" {
  value = google_cloud_run_v2_service.dashboard.uri
}

output "sql_vm_ip" {
  value = local.sql_ip
}

output "registry" {
  value = local.registry
}

output "airflow_ip" {
  value = google_compute_instance.airflow.network_interface[0].access_config[0].nat_ip
}

output "pipeline_job" {
  value = google_cloud_run_v2_job.pipeline.name
}

output "setup_job" {
  value = google_cloud_run_v2_job.setup.name
}
