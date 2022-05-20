terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.85.0"
    }
  }
}

provider "google" {
  # Configuration options
  project = "finaldemo-349008"
  region = "europe-west2"
  zone = "europe-west2-a"
  credentials = "keys.json"
}











