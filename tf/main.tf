

# Set up Cloud Function for air-pollution API data extraction
resource "google_cloudfunctions_function" "air-pollution-api-function" {
  name                  = "air-pollution-api-function"
  description           = "Function to retrieve data from Openweather API"
  runtime               = "python311"
  available_memory_mb   = 128
  source_repository {
    url = "https://source.developers.google.com/projects/${var.gcp_project}/repos/${var.repository_name}/moveable-aliases/${var.branch_name}/paths/${var.source_directory}"
  }
  trigger_http          = true
  entry_point           = "gcloud_get_openweather_data_function"
}


# Create Pub/Sub topic 
# resource "google_pubsub_topic" "basic_topic" {
#   name = "example-topic"
# }

# resource "google_pubsub_subscription" "basic_subscription" {
#   name  = "example-subscription"
#   topic = google_pubsub_topic.basic_topic.name
# }


# # Create Workflow to manage data flow from Cloud Function to Pub/Sub
# resource "google_workflows_workflow" "basic_workflow" {
#   name    = "example-workflow"
#   region  = "us-central1"

#   source_contents = file("${path.module}/path/to/your/workflow.yaml")
# }
