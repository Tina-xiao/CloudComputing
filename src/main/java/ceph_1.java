
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ceph_1 {
    public static void main(String[] args) throws IOException {
        // Ceph S3连接设置
        String accessKey = "test1";
        String secretKey = "test1";
        String endpoint = "http://172.19.241.22:7480";
        String bucketName = "testbucket";
        //String objectKey = "caseObject";
        String prefix="p02547";

//        ClientConfiguration clientConfig = new ClientConfiguration();
//        clientConfig.setProtocol(Protocol.HTTP);

        // 初始化aws s3客户端
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withEndpointConfiguration(new AmazonS3ClientBuilder.EndpointConfiguration(endpoint, "region"))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(new ClientConfiguration())
                .build();

        boolean exists = s3Client.doesBucketExistV2(bucketName);

        if (!exists){
            CreateBucketRequest request = new CreateBucketRequest(bucketName);
            s3Client.createBucket(request);
            System.out.println(exists);



        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName("your-bucketname");
        listObjectsRequest.setDelimiter("/");
        listObjectsRequest.setPrefix("a/");
        ObjectListing listing = s3Client.listObjects(listObjectsRequest);
// 遍历所有Object
        System.out.println("Objects:");
        for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {
            System.out.println(objectSummary.getKey());
        }
// 遍历所有CommonPrefix
        System.out.println("CommonPrefixs:");
        for (String commonPrefix : listing.getCommonPrefixes()) {
            System.out.println(commonPrefix);
        }



        List<ByteArrayOutputStream> filesList = new ArrayList<>();

        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;

        do {
            result = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                S3Object object = s3Client.getObject(bucketName, objectSummary.getKey());
                InputStream objectData = object.getObjectContent();

                // 将 InputStream 转换为 ByteArrayOutputStream
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                int nRead;
                byte[] data = new byte[1024];
                while ((nRead = objectData.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, nRead);
                }

                filesList.add(buffer);

                // 重要：关闭流以避免资源泄露
                objectData.close();
            }

            req.setContinuationToken(result.getNextContinuationToken());
        } while(result.isTruncated());








/*
        // 获取桶中符合前缀的所有对象
        List<S3ObjectSummary> objects = s3Client.listObjects(bucketName, prefix).getObjectSummaries();

// 创建一个List数组用于存储下载的文件
        List<File> files = new ArrayList<>();

// 遍历每个对象，下载到本地临时文件，并添加到List数组中
        for (S3ObjectSummary object : objects) {
            // 获取对象的键名
            String key = object.getKey();
            // 获取对象的输入流
            S3ObjectInputStream inputStream = s3Client.getObject(bucketName, key).getObjectContent();
            // 创建一个本地临时文件
            File file = File.createTempFile(key, null);
            // 将输入流写入到临时文件中
            org.apache.commons.io.FileUtils.copyInputStreamToFile(inputStream, file);
            // 关闭输入流
            inputStream.close();
            // 将临时文件添加到List数组中
            files.add(file);
        }

// 打印List数组的大小和内容
        System.out.println("List size: " + files.size());
        for (File file : files) {
            System.out.println("File name: " + file.getName());
            System.out.println("File size: " + file.length());
        }
*/

    }
    /*
    public static void downloadFromS3(AmazonS3 s3Client,String bucketName,String key,String targetFilePath){
        S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
        if(object!=null){
            System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
            InputStream input = null;
            FileOutputStream fileOutputStream = null;
            byte[] data = null;
            try {
                //获取文件流
                input=object.getObjectContent();
                data = new byte[input.available()];
                int len = 0;
                fileOutputStream = new FileOutputStream(targetFilePath);
                while ((len = input.read(data)) != -1) {
                    fileOutputStream.write(data, 0, len);
                }
                System.out.println("下载文件成功");
            } catch (IOException e) {
                e.printStackTrace();
            }finally{
                if(fileOutputStream!=null){
                    try {
                        fileOutputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if(input!=null){
                    try {
                        input.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
*/

}}
