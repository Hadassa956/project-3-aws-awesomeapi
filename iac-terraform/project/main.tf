#-------------IAM----------------
resource "aws_iam_role" "glue_service_role" {
  name = "${var.project_name}-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

/*---------------default policy attachment----------------
This policy allows the Glue service to access AWS resources on your behalf.
 It includes permissions for reading and writing to S3, 
 as well as permissions for logging and monitoring.
*/
resource "aws_iam_role_policy_attachment" "glue_aws_service" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

/*---------------custom policy attachment----------------
This policy allows the Glue service to access AWS resources where the permission above is not sufficient.
Example: Access your private VPC, access to specific S3 buckets, etc.
*/
resource "aws_iam_policy" "glue_custom_policy" {
  name        = "${var.project_name}-glue-s3-custom-policy"
  description = "Custom policy for AWS Glue to access specific resources"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,       # Referencing the ARN of the S3 bucket created above to allow access to the entire bucket
          "${aws_s3_bucket.data_lake.arn}/*" # Referencing the dinamic ARN of the S3 bucket with a wildcard to allow access to all objects within the bucket

        ]
      }
    ]
  })

}

#----------anexing the custom policy to the role----------------
resource "aws_iam_role_policy_attachment" "glue_custom_policy_attachment" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_custom_policy.arn
}

#------------------------s3 buckets------------------------
resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.project_name}-data-lake-072"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Bronze layer: Raw data, unprocessed, directly from the source.
resource "aws_s3_object" "data_lake_bronze" {
  bucket       = aws_s3_bucket.data_lake.id # Here you reference the S3 bucket created above using its ID to create the object in the same bucket.
  key          = "bronze/"                  #create a folder bronze and the file name"
  content_type = "application/x-directory"  # To advise that this is a directory, not a file and the s3 consider it as a folder.
}
# Silver layer: Cleaned and transformed data, ready for analysis.
resource "aws_s3_object" "data_lake_silver" {
  bucket       = aws_s3_bucket.data_lake.id
  key          = "silver/"
  content_type = "application/x-directory"
}
# Gold layer: Curated and enriched data, optimized for business intelligence and reporting.
resource "aws_s3_object" "data_lake_gold" {
  bucket       = aws_s3_bucket.data_lake.id
  key          = "gold/"
  content_type = "application/x-directory"
}

#------------------------glue catalog------------------------
resource "aws_glue_catalog_database" "data_lake_db" {
  name        = "${var.project_name}_data_lake_db"
  description = "Glue Catalog Database for the Data Lake"
}

#------------------------glue crawler-bronze layer------------------------
resource "aws_glue_crawler" "bronze_crawler" {
  name          = "${var.project_name}-bronze-crawler"
  database_name = aws_glue_catalog_database.data_lake_db.name
  role          = aws_iam_role.glue_service_role.arn #It takes the ARN of the IAM role created above to allow the crawler to access the necessary resources.

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/"
  }
  description = "Glue Crawler for the Bronze layer raw data"
}

