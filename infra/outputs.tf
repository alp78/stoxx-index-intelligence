output "dashboard_url" {
  value = google_cloud_run_v2_service.dashboard.uri
}

output "sql_private_ip" {
  value = google_sql_database_instance.main.private_ip_address
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
