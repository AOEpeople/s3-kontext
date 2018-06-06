package com.aoe.s3kontext

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.CopyObjectResult
import com.amazonaws.services.s3.model.PutObjectResult
import com.natpryce.konfig.ConfigurationProperties.Companion.systemProperties
import com.natpryce.konfig.EnvironmentVariables
import com.natpryce.konfig.getValue
import com.natpryce.konfig.overriding
import com.natpryce.konfig.stringType
import org.apache.commons.io.input.BOMInputStream

/**
 * You can use the Context Builder via systemProperties, EnvironmentVariables or via Constructor Arguments.
 * If the Constructor Arguments s3Client leave empty, it try to grab the information with these properties:
 * S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET, S3_REGION, AWS_ENDPOINT
 * AWS_ENDPOINT is optional and only used for overwriting the aws endpoint e.g. for mocking s3
 *
 */
@Suppress("VariableNaming", "ClassNaming")
class S3ContextBuilder(bucketName: String = "", s3Client: AmazonS3? = null) {
    private val s3Config = systemProperties() overriding
        EnvironmentVariables()

    /**
     * used s3Client
     */
    val s3Client: AmazonS3 by lazy {
        when {
            s3Client != null -> s3Client
            else -> AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
                .withCredentials(
                    AWSStaticCredentialsProvider(
                        BasicAWSCredentials(s3Config[S3_ACCESS_KEY_ID], s3Config[S3_SECRET_ACCESS_KEY])
                    )
                )
                .apply {
                    when {
                        s3Config[AWS_ENDPOINT].isEmpty() -> this.withRegion(s3Config[S3_REGION])
                        else -> this.withEndpointConfiguration(
                            AwsClientBuilder.EndpointConfiguration(s3Config[AWS_ENDPOINT], s3Config[S3_REGION])
                        )
                    }
                }
                .build()
        }
    }

    private val bucketName by lazy {
        when {
            bucketName.isEmpty() -> s3Config[S3_BUCKET]
            else -> bucketName
        }
    }

    /**
     * starts a context to using other methods
     */
    fun context(build: S3ContextBuilder.() -> Unit) = this.build()

    /**
     * move object to destination
     */
    infix fun String.moveTo(destinationKey: String) {
        this copyTo destinationKey
        this.delete()
    }

    /**
     * copy object to destination
     */
    infix fun String.copyTo(destinationKey: String): CopyObjectResult =
        s3Client.copyObject(bucketName, this, bucketName, destinationKey)

    /**
     * Put string content as object to s3
     */
    infix fun String.putTo(destinationKey: String): PutObjectResult =
        s3Client.putObject(bucketName, destinationKey, this)

    /**
     * delete object from bucket
     */
    fun String.delete() = s3Client.deleteObject(bucketName, this)

    /**
     * checks if object exists in bucket
     */
    fun String.exists() = s3Client.doesObjectExist(bucketName, this)

    /**
     * get a reader on an object
     */
    fun String.reader() = BOMInputStream(getObject().objectContent).reader()

    /**
     * get object
     */
    fun String.getObject() = s3Client.getObject(bucketName, this)

    /**
     * list all object
     */
    @JvmOverloads
    fun listObjects(prefix: String? = null, suffix: String? = null, skipEmpty: Boolean = false) = s3Client
        .listObjects(bucketName, prefix)
        .objectSummaries.run {
        when (suffix) {
            is String -> filter { it.key.endsWith(suffix) }
            else -> this
        }
    }.run {
        when (skipEmpty) {
            true -> filterNot { it.eTag == EMPTY_VALUE }
            else -> this
        }
    }.toList()

    private val S3_ACCESS_KEY_ID by stringType
    private val S3_SECRET_ACCESS_KEY by stringType
    private val S3_BUCKET by stringType
    private val S3_REGION by stringType
    private val AWS_ENDPOINT by stringType

    companion object {
        private const val EMPTY_VALUE = "d41d8cd98f00b204e9800998ecf8427e"
    }
}

/**
 * shortcut to initialize a S3ContextBuilder
 */
fun s3(bucketName: String = "", s3Client: AmazonS3? = null, build: S3ContextBuilder.() -> Unit) =
    S3ContextBuilder(bucketName, s3Client).context(build)
