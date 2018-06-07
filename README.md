s3-konext - A DSL for Amazon S3
=======================================

Easy Access to Amazon S3 with content fluent operations via kotlin DSL.
Wrapping the offical AWS Java Client to access the content.

Usage
----
```kotlin
s3 {
    //copy
    "some/path/somekey" copyTo "some/other/path/somekey"
    //move
    "this is some content" putTo "some/file.txt"
    //list
    val entries = listObjects("some/path")
    //list with filter
    val csvFiles = listObjects("some/path",".csv")
    //list remove empty files
    val onlyContentMatters = listObjects("some/path", skipEmpty=true)
    //reading content (BOM-save input stream reader)
    val content = "some/path/to/file".reader().readText()
}
```

Configuration
----
Using the awesome [konfig](https://github.com/npryce/konfig) framework you can configure the following values that will be used in the s3 kontext. The order here is System Properties override Enviorment Variables

| Name                  | Use                       |
| -------------         |-------------              |
| S3_ACCESS_KEY_ID      | access key to use         |
| S3_SECRET_ACCESS_KEY  | secret to use             |
| S3_BUCKET             | the bucket (optional), can be passed as parameter see below     |
| S3_REGION             | region to use or          |
| AWS_ENDPOINT          | actual endpoint to use    |

I already got a AmazonS3 Client instance on Hand
-----
If you are already have a (com.amazonaws.services.s3.)AmazonS3 instance you can use it like this
```kotlin
val someAmazonS3 : AmazonS3 = getFromSomewhere()
s3(s3Client = someAmazonS3) {
    "some/path/somekey" copyTo "some/other/path/somekey"
    ...
}
```
_EXTRA:_ Id like to define the bucket name dynamically
-----
No problem... just pass it in when you create the s3 kontext
```kotlin

s3(bucketName = "some bucket name") {
    "oh, this is so convenient" putTo "list-of-facts/proven.txt"
    ...
}
```
