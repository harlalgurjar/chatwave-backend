const cloudinary = require("cloudinary").v2;

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

const MAX_IMAGE_BYTES = 8 * 1024 * 1024; // ~8MB
const MAX_VIDEO_BYTES = 30 * 1024 * 1024; // ~30MB
const MAX_FILE_BYTES = 20 * 1024 * 1024; // ~20MB

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

// Status updates can be a photo OR a short video — this covers both, and
// picks the right Cloudinary resource type for each.
async function uploadStatusMediaFromDataUrl(dataUrl) {
  if (!dataUrl || typeof dataUrl !== "string") throw new Error("Invalid media data");
  const isVideo = dataUrl.startsWith("data:video/");
  const isImage = dataUrl.startsWith("data:image/");
  if (!isVideo && !isImage) throw new Error("Only photo or video status is supported");

  const approxBytes = dataUrl.length * 0.75;
  const limit = isVideo ? MAX_VIDEO_BYTES : MAX_IMAGE_BYTES;
  if (approxBytes > limit) {
    throw new Error(isVideo ? "Video is too large (max ~30MB)" : "Image is too large (max ~8MB)");
  }

  const result = await cloudinary.uploader.upload(dataUrl, {
    folder: "chatwave/status",
    resource_type: isVideo ? "video" : "image",
  });
  return { url: result.secure_url, type: isVideo ? "video" : "image" };
}

// Any other file (PDF, doc, zip, etc.) sent in a chat — stored as a "raw"
// resource on Cloudinary since it isn't an image or video.
async function uploadFileFromDataUrl(dataUrl, fileName) {
  if (!dataUrl || typeof dataUrl !== "string" || !dataUrl.startsWith("data:")) {
    throw new Error("Invalid file data");
  }
  const approxBytes = dataUrl.length * 0.75;
  if (approxBytes > MAX_FILE_BYTES) {
    throw new Error("File is too large (max ~20MB)");
  }

  // Cloudinary auto-detects images/videos and stores everything else as
  // "raw" so plain documents don't get rejected or mangled.
  const result = await cloudinary.uploader.upload(dataUrl, {
    folder: "chatwave/files",
    resource_type: "auto",
    use_filename: true,
    filename_override: fileName || undefined,
  });
  return result.secure_url;
}

module.exports = { uploadImageFromDataUrl, uploadStatusMediaFromDataUrl, uploadFileFromDataUrl };
