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
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::aws-awesomeapi-bucket/*",
          "arn:aws:s3:::aws-awesomeapi-gold-bucket/*"
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
    bucket = "${var.project_name}-data-lake-072"
    force_destroy = true
}

resource "aws_s3_bucket" "data_lake_bronze" {
    bucket = "aws_s3_bucket.data_lake-072-bronze"
    force_destroy = true
}

resource "aws_s3_bucket" "data_lake_silver" {
    bucket = "aws_s3_bucket.data_lake-072-silver"
    force_destroy = true
}

resource "aws_s3_bucket" "data_lake_gold" {
    bucket = "aws_s3_bucket.data_lake-072-gold"
    force_destroy = true
}
