provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "bootstrap_bucket" {
  bucket        = "bootstrap-bucket-072-hadassa"
  force_destroy = true
}