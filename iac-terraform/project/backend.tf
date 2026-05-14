terraform {
  backend "s3" {
    bucket = "bootstrap-bucket-072-hadassa"
    key    = "project/terraform.tfstate"
    region = "us-east-1"

    use_lockfile = true
  }
}