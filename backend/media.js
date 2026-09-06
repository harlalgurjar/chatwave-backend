const cloudinary = require("cloudinary").v2;

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

const MAX_IMAGE_BYTES = 8 * 1024 * 1024; // ~8MB

async function uploadImageFromDataUrl(dataUrl) {
  if (!dataUrl || typeof dataUrl !== "string" || !dataUrl.startsWith("data:image/")) {
    throw new Error("Invalid image data");
  }
  // Rough byte-size estimate from the base64 string length, so we reject
  // an obviously oversized upload before even sending it to Cloudinary.
  const approxBytes = dataUrl.length * 0.75;
  if (approxBytes > MAX_IMAGE_BYTES) {
    throw new Error("Image is too large (max ~8MB)");
  }

  const result = await cloudinary.uploader.upload(dataUrl, {
    folder: "chatwave",
    resource_type: "image",
  });
  return result.secure_url;
}

module.exports = { uploadImageFromDataUrl };
