
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

//把ceph的内容提成list
public class ceph {
    public static List<ByteArrayOutputStream> getList() throws IOException {
        // Ceph S3连接设置
        String accessKey = "test1";
        String secretKey = "test1";
        String endpoint = "http://172.19.241.22:7480";
        String bucketName = "testbucket";
        //String objectKey = "caseObject";
        String prefix = "p02547";


        // 初始化aws s3客户端
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withEndpointConfiguration(new AmazonS3ClientBuilder.EndpointConfiguration(endpoint, ""))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(new ClientConfiguration())
                .build();


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

//        for(int i = 0; i < filesList.size(); i++){
//            System.out.println(filesList.get(i));
//        }
        return filesList;

    }
    public static void main(String[] args) throws IOException {

      List<ByteArrayOutputStream> res = getList();
        for(int i = 0; i < res.size(); i++){
            if(i%50 == 0)
            System.out.println(res.get(i));
        }

    }}