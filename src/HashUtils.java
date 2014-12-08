import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class HashUtils {
	public static String genMD5(String message) {
		return hashString(message, "MD5");
	}
	
	public static String genSHA1(String message) {
		return hashString(message, "SHA-1");
	}
	
	public static String genSHA256(String message) {
		return hashString(message, "SHA-256");
	}
	
	public static String genMD5(File file) {
	    return hashFile(file, "MD5");
	}
	 
	public static String genSHA1(File file) {
	    return hashFile(file, "SHA-1");
	}
	 
	public static String genSHA256(File file) {
	    return hashFile(file, "SHA-256");
	}
	
	private static String hashFile(File file, String algorithm) {
		try (FileInputStream inputStream = new FileInputStream(file)) {
	        MessageDigest digest = MessageDigest.getInstance(algorithm);
	 
	        byte[] bytesBuffer = new byte[1024];
	        int bytesRead = -1;
	 
	        while ((bytesRead = inputStream.read(bytesBuffer)) != -1) {
	            digest.update(bytesBuffer, 0, bytesRead);
	        }
	 
	        byte[] hashedBytes = digest.digest();
	 
	        return convertByteArrayToHexString(hashedBytes);
	    } catch (IOException | NoSuchAlgorithmException ex) {
	    	ex.printStackTrace();
	    }
		return null;
	}
	
	private static String hashString(String str, String algorithm) {
		try {
			MessageDigest digest = MessageDigest.getInstance(algorithm);
			byte[] hashedBytes = digest.digest(str.getBytes());
			
			return convertByteArrayToHexString(hashedBytes);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static String convertByteArrayToHexString(byte[] arrayBytes) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < arrayBytes.length; i++) {
			stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return stringBuffer.toString();
	}
}

