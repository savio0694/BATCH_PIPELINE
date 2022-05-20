
resource "google_storage_bucket" "GCS1" {
  
  name = "landingbucket0694"
  storage_class = "STANDARD"
  location = "EUROPE-WEST2"
  labels = {
    "env" = "tf_env"
    "usecase" = "landing_storage"
  }
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 5
    }
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  

}

resource "google_storage_bucket" "GCS2" {
  
  name = "cleanbucket0694"
  storage_class = "STANDARD"
  location = "EUROPE-WEST2"
  labels = {
    "env" = "tf_env"
    "usecase" = "landing_storage"
  }
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 15
    }
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  

}


resource "google_storage_bucket" "GCSSPARK" {
  
  name = "sparkbucketbucket0694"
  storage_class = "STANDARD"
  location = "EUROPE-WEST2"
  labels = {
    "env" = "tf_env"
    "usecase" = "spark_job__storage"
  }
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 15
    }
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  

}

resource "google_bigquery_table" "cust" {
  dataset_id = "final_demo_data"
  table_id   = "customer"

  

  schema = <<EOF
[{
    "name": "id",
    "type": "INTEGER",
    "mode": "NULLABLE"
    
  },
  {
    "name": "name",
    "type": "STRING",
    "mode": "NULLABLE"
    
  },
  {
    "name": "date",
    "type": "DATE",
    "mode": "NULLABLE"
    
  },
  {
    "name": "country",
    "type": "STRING",
    "mode": "NULLABLE"
    
  },
  {
    "name": "qty",
    "type": "INTEGER",
    "mode": "NULLABLE"
    
  },
  {
    "name": "sale_amt",
    "type": "INTEGER",
    "mode": "NULLABLE"
    
  },
  {
    "name": "COMPANY",
    "type": "STRING",
    "mode": "NULLABLE"
    
  },
  {
    "name": "sales",
    "type": "INTEGER",
    "mode": "NULLABLE"
    
  }
  
]

EOF
}



