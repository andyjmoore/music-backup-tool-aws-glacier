package uk.me.andjmoo.musicbackuptoolglacier.app;

/**
 * Some common part sizes for a multi-part upload
 * 
 * @see <a href=
 *      "https://docs.aws.amazon.com/amazonglacier/latest/dev/uploading-archive-mpu.html">https://docs.aws.amazon.com/amazonglacier/latest/dev/uploading-archive-mpu.html</a>
 */
public enum PartSizes {
	ONE_MB(1048576), ONE_HUNDRED_MB(ONE_MB.size() * 128);

	/**
	 * Min part size of 1 MB
	 */
	public static final int MIN_PART_SIZE_BYTES = 1048576;

	/**
	 * Max part size of 4 GB
	 */
	public static final long MAX_PART_SIZE_BYTES = 4294967296L;

	private final int size;

	PartSizes(int partSize) {
		if (isValidPartSize(partSize)) {
			this.size = partSize;
		} else {
			throw new IllegalArgumentException("Part size '" + partSize + "' invalid");
		}
	}

	public int size() {
		return this.size;
	}

	/**
	 * Part size specs: <blockquote> 1 MB to 4 GB, last part can be < 1 MB. You
	 * specify the size value in bytes. The part size must be a megabyte (1024 KB)
	 * multiplied by a power of 2. For example, 1048576 (1 MB), 2097152 (2 MB),
	 * 4194304 (4 MB), 8388608 (8 MB). </blockquote>
	 * 
	 * @see <a href=
	 *      "https://docs.aws.amazon.com/amazonglacier/latest/dev/uploading-archive-mpu.html">https://docs.aws.amazon.com/amazonglacier/latest/dev/uploading-archive-mpu.html</a>
	 * @param partSize
	 */
	public static boolean isValidPartSize(int partSize) {
		return (partSize >= MIN_PART_SIZE_BYTES) && (partSize <= MAX_PART_SIZE_BYTES) && isPowerOf2(partSize);
	}

	public static boolean isPowerOf2(int x) {
		return Integer.bitCount(x) == 1;
	}
}
