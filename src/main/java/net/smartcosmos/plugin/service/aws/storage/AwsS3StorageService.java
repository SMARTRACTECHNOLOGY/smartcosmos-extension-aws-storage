package net.smartcosmos.plugin.service.aws.storage;

/*
 * *#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
 * SMART COSMOS AWS S3 Storage Service Plugin
 * ===============================================================================
 * Copyright (C) 2013 - 2015 Smartrac Technology Fletcher, Inc.
 * ===============================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#
 */

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Preconditions;
import net.smartcosmos.objects.model.context.IFile;
import net.smartcosmos.platform.api.service.IStorageService;
import net.smartcosmos.platform.base.AbstractAwsService;
import net.smartcosmos.platform.pojo.service.StorageRequest;
import net.smartcosmos.platform.pojo.service.StorageResponse;
import net.smartcosmos.platform.util.HashingInputStream;
import net.smartcosmos.util.HashUtil;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class AwsS3StorageService extends AbstractAwsService<AWSCredentials>
        implements IStorageService
{
    private static final Logger LOG = LoggerFactory.getLogger(AwsS3StorageService.class);

    public AwsS3StorageService()
    {
        super("8AC7970C42538B3B0142538D42D9000C", "AWS S3 Storage Service");
    }

    private String getBucketName()
    {
        return context.getConfiguration().getServiceParameters().get("s3bucketName");
    }

    @Override
    public InputStream retrieve(IFile file) throws IOException
    {
        Preconditions.checkArgument((file != null), "file must not be null");

        AmazonS3 s3 = new AmazonS3Client(credentials, new ClientConfiguration().withProtocol(Protocol.HTTPS));

        GetObjectRequest getObjectRequest = new GetObjectRequest(getBucketName(), file.getFileName());
        S3Object storedObject = s3.getObject(getObjectRequest);

        return storedObject.getObjectContent();
    }

    @Override
    public void delete(IFile file) throws IOException
    {
        AmazonS3 s3 = new AmazonS3Client(credentials, new ClientConfiguration().withProtocol(Protocol.HTTPS));

        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(getBucketName(), file.getFileName());
        s3.deleteObject(deleteObjectRequest);
    }

    @Override
    public StorageResponse store(StorageRequest request) throws IOException
    {
        StorageResponse response = new StorageResponse();

        AmazonS3 s3 = new AmazonS3Client(credentials, new ClientConfiguration().withProtocol(Protocol.HTTPS));

        Map<String, String> fileMetadata = new HashMap<String, String>();

        fileMetadata.put("accountUrn", request.getUser().getAccount().getUrn());
        fileMetadata.put("userUrn", request.getUser().getUrn());
        fileMetadata.put("fileUrn", request.getFile().getUrn());
        fileMetadata.put("entityReferenceType", request.getFile().getEntityReferenceType().name());
        fileMetadata.put("referenceUrn", request.getFile().getReferenceUrn());
        fileMetadata.put("recordedTimestamp", Long.toString(request.getFile().getTimestamp()));
//            fileMetadata.put("mimeType", request.getVfsObject().getMimeType());

        ObjectMetadata metadata = new ObjectMetadata();
        if (request.getContentLength() > 0)
        {
            LOG.debug("Including content length : " + request.getContentLength());
            metadata.setContentLength(request.getContentLength());
        }
//            metadata.setContentMD5(streamMD5);
        metadata.setUserMetadata(fileMetadata);

        try
        {
            LOG.trace("Bucket name: " + getBucketName());
            LOG.trace("File name: " + request.getFileName());
            LOG.trace("inputStream == null?  " + (request.getInputStream() == null));

            HashingInputStream his = new HashingInputStream(request.getInputStream(), "SHA-256");

            PutObjectResult putResult = s3.putObject(new PutObjectRequest(getBucketName(),
                    request.getFileName(),
                    his,
                    metadata));


            String finalUrl = getUrl(request.getFileName());
            LOG.trace("File URL: " + finalUrl);

            response.setUrl(finalUrl);

            request.getFile().setUrl(getUrl(request.getFileName()));

            byte[] signature = his.getSignature();

            JSONObject jsonObject = HashUtil.signFile(request.getFile(), signature);
            LOG.info("File Signature\n\n{}\n\n", jsonObject.toString(3));

            response.setContentHash(jsonObject.toString(3));

        } catch (AmazonS3Exception e)
        {
            e.printStackTrace();
            throw e;
        } catch (JSONException | NoSuchAlgorithmException e)
        {
            e.printStackTrace();
            throw new IOException(e);
        }

        return response;
    }

    protected String getUrl(String fileName)
    {
        String url = "https://" + getBucketName() + ".s3.amazonaws.com/" + fileName;
        LOG.trace("URL: " + url);
        return url;
    }

    @Override
    public boolean isHealthy()
    {
        AmazonS3 s3 = new AmazonS3Client(credentials, new ClientConfiguration().withProtocol(Protocol.HTTPS));
        return s3.doesBucketExist(getBucketName());
    }

    @Override
    protected AWSCredentials createCloudCredentials(String accessKey, String secretAccessKey)
    {
        return new BasicAWSCredentials(accessKey, secretAccessKey);
    }
}
