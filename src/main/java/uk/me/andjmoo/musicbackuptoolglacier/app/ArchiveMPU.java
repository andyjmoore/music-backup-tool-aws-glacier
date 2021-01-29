package uk.me.andjmoo.musicbackuptoolglacier.app;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.glacier.GlacierClient;
import software.amazon.awssdk.services.glacier.GlacierClientBuilder;
import software.amazon.awssdk.services.glacier.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.glacier.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.glacier.model.DescribeVaultRequest;
import software.amazon.awssdk.services.glacier.model.DescribeVaultResponse;
import software.amazon.awssdk.services.glacier.model.InitiateMultipartUploadRequest;
import software.amazon.awssdk.services.glacier.model.InitiateMultipartUploadResponse;
import software.amazon.awssdk.services.glacier.model.UploadMultipartPartRequest;
import software.amazon.awssdk.services.glacier.model.UploadMultipartPartResponse;
import software.amazon.awssdk.utils.BinaryUtils;

/**
 * Example taken from
 * https://docs.aws.amazon.com/amazonglacier/latest/dev/uploading-an-archive-mpu-using-java.html
 * and updated for latest AWS Java SDK
 *
 */
public class ArchiveMPU {

	private static Logger logger = LoggerFactory.getLogger(ArchiveMPU.class);

	public static String vaultName = "vaultName"; // update this for real vault name
	// This example works for part sizes up to 1 GB.
	public static int partSize = PartSizes.ONE_HUNDRED_MB.size();
	public static String archiveFilePath = "archiveFilePath"; // update this for path to archive to upload
	public static GlacierClientBuilder glacierClientBuilder = GlacierClient.builder();
	public static GlacierClient glacierClient;

	public static void main(String[] args) throws IOException {

		ProfileCredentialsProvider credentials = ProfileCredentialsProvider.builder().build();

		glacierClient = glacierClientBuilder.credentialsProvider(credentials).build();

		try {
			System.out.println("Uploading an archive.");
			String uploadId = initiateMultipartUpload();
			String checksum = uploadParts(uploadId);
			String archiveId = completeMultiPartUpload(uploadId, checksum);
			System.out.println("Completed an archive. ArchiveId: " + archiveId);

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e);
		}

	}

	private static String initiateMultipartUpload() {
		// Initiate
		InitiateMultipartUploadRequest request = InitiateMultipartUploadRequest.builder().vaultName(vaultName)
				.archiveDescription("my archive " + (new Date())).partSize(String.valueOf(partSize)).build();

		InitiateMultipartUploadResponse result = glacierClient.initiateMultipartUpload(request);

		System.out.println("ArchiveID: " + result.uploadId());
		return result.uploadId();
	}

	private static String uploadParts(String uploadId) throws NoSuchAlgorithmException, IOException {

		if (!PartSizes.isValidPartSize(partSize)) {
			throw new IllegalArgumentException("Part size '" + partSize + "' invalid");
		}
		
		int filePosition = 0;
		long currentPosition = 0;
		byte[] buffer = new byte[Integer.valueOf(partSize)];

		File file = new File(archiveFilePath);
		// TODO Maybe inefficient to read the file to calc the chechsums, and then re-read the file to actually send the data...
		byte[][] binaryChecksumsForWholeFile = TreeHashUtil.getChunkSHA256Hashes(file);

		try (FileInputStream fileToUpload = new FileInputStream(file)) {
			String contentRange;
			int read = 0;
			while (currentPosition < file.length()) {
				read = fileToUpload.read(buffer, filePosition, buffer.length);
				if (read == -1) {
					break;
				}
				byte[] bytesRead = Arrays.copyOf(buffer, read);

				contentRange = String.format("bytes %s-%s/*", currentPosition, currentPosition + read - 1);
				byte[][] rangeChecksums = TreeHashUtil.getChunkSHA256Hashes(bytesRead);
				String checksum = TreeHashUtil.toHex(TreeHashUtil.computeSHA256TreeHash(rangeChecksums));
				logger.info("Preparing part contentRange={} checksum={}", contentRange, checksum);

				UploadMultipartPartRequest partRequest = UploadMultipartPartRequest.builder().vaultName(vaultName)
						.checksum(checksum).range(contentRange).uploadId(uploadId).build();
				RequestBody requestBody = RequestBody.fromBytes(bytesRead);

				UploadMultipartPartResponse partResult = glacierClient.uploadMultipartPart(partRequest, requestBody);
				System.out.println("Part uploaded, response checksum: " + partResult.checksum());

				currentPosition = currentPosition + read;
			}
		}
		return TreeHashUtil.toHex(TreeHashUtil.computeSHA256TreeHash(binaryChecksumsForWholeFile));
	}

	private static String completeMultiPartUpload(String uploadId, String checksum)
			throws NoSuchAlgorithmException, IOException {

		File file = new File(archiveFilePath);

		CompleteMultipartUploadRequest compRequest = CompleteMultipartUploadRequest.builder().vaultName(vaultName).checksum(checksum)
				.uploadId(uploadId).archiveSize(String.valueOf(file.length())).build();

		CompleteMultipartUploadResponse compResult = glacierClient.completeMultipartUpload(compRequest);
		return compResult.location();
	}
}