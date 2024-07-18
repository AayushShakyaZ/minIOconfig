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

     public String getUrl(String objectName) throws IOException, NoSuchAlgorithmException, InvalidKeyException, MinioException {

        String url = minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(bucketName)
                        .object(objectName)
                        .build());
        logger.info("Calling minio logo folder for fetching respective logos {}"+url);
        return url;
    }


}
