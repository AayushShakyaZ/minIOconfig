package com.nchl.connectips.minIO.service;

import io.minio.GetObjectArgs;
import io.minio.GetPresignedObjectUrlArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import io.minio.http.Method;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class MinioService {

    final static Logger logger = Logger.getLogger(MinioService.class);

    @Autowired
    private MinioClient minioClient;

    @Value("${minio.bucketName}")
    private String bucketName;

    public InputStream getFile(String sourceFile) throws Exception {
        try {
            logger.info("Fetching files from bucket's folder");
            return minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(sourceFile)
                            .build());

        } catch (MinioException e) {
            throw new Exception("Error downloading file from MinIO: " + e.getMessage(), e);
        }
    }

    public String getUrl(String bucketName, String objectName) throws IOException, NoSuchAlgorithmException, InvalidKeyException, MinioException {
        Map<String, String> reqParams = new HashMap<>();
        reqParams.put("response-content-type", "application/json");
        String url = minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(bucketName)
                        .object(objectName)
                        .expiry(2, TimeUnit.HOURS)
                        .extraQueryParams(reqParams)
                        .build());
        logger.info("Calling url of NCHL logo");
        return url;
    }

    public String getBankLogoUrl(String bucketName, String objectName, String bankId) throws IOException, NoSuchAlgorithmException, InvalidKeyException, MinioException {
        Map<String, String> reqParams = new HashMap<>();
        String bankLogoPath = objectName+"/"+bankId+".png";
        String url = minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(bucketName)
                        .object(bankLogoPath)
                        .expiry(2, TimeUnit.HOURS)
                        .extraQueryParams(reqParams)
                        .build());
        logger.info("Calling url of logo based on bankID");
        return url;
    }


}
