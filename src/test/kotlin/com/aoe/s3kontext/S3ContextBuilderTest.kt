package com.aoe.s3kontext

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.AnonymousAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.findify.s3mock.S3Mock
import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldNotBe
import io.kotlintest.specs.StringSpec

class S3ContextBuilderTest : StringSpec({

    "should invoke exist for string" {
        //setup
        System.setProperty("S3_ACCESS_KEY_ID", "foo")
        System.setProperty("S3_SECRET_ACCESS_KEY", "bar")
        System.setProperty("S3_BUCKET", TEST_BUCKET)
        System.setProperty("S3_REGION", "us-west-2")
        System.setProperty("AWS_ENDPOINT", "http://localhost:8001")

        s3 {
            //setup
            s3Client.createBucket(TEST_BUCKET)
            s3Client.putObject(TEST_BUCKET, FILENAME, "contents")
        }

        //test
        s3 {
            FILENAME.exists() shouldBe true
            "some/outher/file".exists() shouldBe false
        }

        //clean up
        s3 {
            s3Client.deleteBucket(TEST_BUCKET)
        }
    }
    "should be able to put files" {
        //setup:
        System.setProperty("S3_ACCESS_KEY_ID", "foo")
        System.setProperty("S3_SECRET_ACCESS_KEY", "bar")
        System.setProperty("S3_BUCKET", TEST_BUCKET)
        System.setProperty("S3_REGION", "us-west-2")
        System.setProperty("AWS_ENDPOINT", "http://localhost:8001")

        s3 {
            s3Client.createBucket(TEST_BUCKET)
            "content" putTo FILENAME
        }

        //expect:
        s3 {
            FILENAME.exists() shouldBe true
        }

        //clean up:
        s3 {
            s3Client.deleteBucket(TEST_BUCKET)
        }
    }

    "should be able to list files in folders" {
        //setup:
        System.setProperty("S3_ACCESS_KEY_ID", "foo")
        System.setProperty("S3_SECRET_ACCESS_KEY", "bar")
        System.setProperty("S3_BUCKET", TEST_BUCKET)
        System.setProperty("S3_REGION", "us-west-2")
        System.setProperty("AWS_ENDPOINT", "http://localhost:8001")

        s3 {
            s3Client.createBucket(TEST_BUCKET)
            "content" putTo FILENAME
        }

        //expect:
        s3 {
            val folderContent = listObjects(FOLDER)
            folderContent.size shouldBe 1
            folderContent.first().key == FILENAME
        }

        //clean up
        s3 {
            s3Client.deleteBucket(TEST_BUCKET)
        }
    }

    "should be able to filter list files in folders" {
        //setup:
        System.setProperty("S3_ACCESS_KEY_ID", "foo")
        System.setProperty("S3_SECRET_ACCESS_KEY", "bar")
        System.setProperty("S3_BUCKET", TEST_BUCKET)
        System.setProperty("S3_REGION", "us-west-2")
        System.setProperty("AWS_ENDPOINT", "http://localhost:8001")

        s3 {
            s3Client.createBucket(TEST_BUCKET)
            "content" putTo FILENAME
            "content" putTo FILENAME2
            "content" putTo FILENAME3
        }

        //expect:
        s3 {
            val folderContent = listObjects(FOLDER, ".csv")
            folderContent.size shouldBe 2
            folderContent[0].key == FILENAME
            folderContent[1].key == FILENAME2
        }

        //cleanup:
        s3 {
            s3Client.deleteBucket(TEST_BUCKET)
        }
    }

    "should be able to skip empty files when filter list files in folders" {
        //setup:
        System.setProperty("S3_ACCESS_KEY_ID", "foo")
        System.setProperty("S3_SECRET_ACCESS_KEY", "bar")
        System.setProperty("S3_BUCKET", TEST_BUCKET)
        System.setProperty("S3_REGION", "us-west-2")
        System.setProperty("AWS_ENDPOINT", "http://localhost:8001")

        s3 {
            s3Client.createBucket(TEST_BUCKET)
            "content" putTo FILENAME
            "" putTo FILENAME2
            "content" putTo FILENAME3
        }

        //expect:
        s3 {
            val folderContent = listObjects(FOLDER, ".csv", true)
            folderContent.size shouldBe 1
            folderContent[0].key shouldBe FILENAME
        }

        s3 {
            s3Client.deleteBucket(TEST_BUCKET)
        }
    }

    "should be able to read files" {
        //setup:
        System.setProperty("S3_ACCESS_KEY_ID", "foo")
        System.setProperty("S3_SECRET_ACCESS_KEY", "bar")
        System.setProperty("S3_BUCKET", TEST_BUCKET)
        System.setProperty("S3_REGION", "us-west-2")
        System.setProperty("AWS_ENDPOINT", "http://localhost:8001")

        s3 {
            s3Client.createBucket(TEST_BUCKET)
            s3Client.putObject(TEST_BUCKET, FILENAME, "contents")
        }

        //expect:
        s3 {
            FILENAME.reader().readText() shouldBe "contents"
        }

        //cleanup:
        s3 {
            s3Client.deleteBucket(TEST_BUCKET)
        }
    }

    "should move file" {
        //setup:
        System.setProperty("S3_ACCESS_KEY_ID", "foo")
        System.setProperty("S3_SECRET_ACCESS_KEY", "bar")
        System.setProperty("S3_BUCKET", TEST_BUCKET)
        System.setProperty("S3_REGION", "us-west-2")
        System.setProperty("AWS_ENDPOINT", "http://localhost:8001")

        s3 {
            s3Client.createBucket(TEST_BUCKET)
            s3Client.putObject(TEST_BUCKET, FILENAME, "contents")
        }

        //expect:
        s3 {
            FILENAME moveTo "someOtherFolder/testfile.csv"
            "someOtherFolder/testfile.csv".exists() shouldBe true
            FILENAME.exists() shouldNotBe true
        }

        //cleanup:
        s3 {
            s3Client.deleteBucket(TEST_BUCKET)
        }
    }


    "use s3 client" {
        //setup:
        val client = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://localhost:8001", "us-west-2"))
            .withCredentials(AWSStaticCredentialsProvider(AnonymousAWSCredentials()))
            .build()
        s3(TEST_BUCKET, client) {
            s3Client.createBucket(TEST_BUCKET)
            s3Client.putObject(TEST_BUCKET, FILENAME, "contents")
        }

        //expect:
        s3(TEST_BUCKET, client) {
            FILENAME moveTo "someOtherFolder/testfile.csv"
            "someOtherFolder/testfile.csv".exists() shouldBe true
            FILENAME.exists() shouldNotBe true
        }

        //cleanup:
        s3 {
            s3Client.deleteBucket(TEST_BUCKET)
        }
    }


    "should use bucket from constructor"() {
        //setup:
        System.setProperty("S3_ACCESS_KEY_ID", "foo")
        System.setProperty("S3_SECRET_ACCESS_KEY", "bar")
        System.setProperty("S3_BUCKET", TEST_BUCKET)
        System.setProperty("S3_REGION", "us-west-2")
        System.setProperty("AWS_ENDPOINT", "http://localhost:8001")

        val bucketName = "anotherbucket"
        s3(bucketName) {
            s3Client.createBucket(bucketName)
            s3Client.putObject(bucketName, FILENAME, "contents")

            s3Client.doesBucketExistV2(bucketName)
            !s3Client.doesBucketExistV2(TEST_BUCKET)

            FILENAME.exists() shouldBe true
            "notExistingFile".exists() shouldNotBe true

            //cleanup:
            s3Client.deleteBucket(bucketName)
        }
    }

}) {
    companion object {
        val s3Mock = S3Mock.Builder().withPort(8001).withInMemoryBackend().build().start()
        const val FOLDER = "someFolder/"
        const val FILENAME = "$FOLDER}testfile.csv"
        const val FILENAME2 = "${FOLDER}testfile2.csv"
        const val FILENAME3 = "${FOLDER}testfile3.png"
        const val TEST_BUCKET = "testbucket2"
    }
}