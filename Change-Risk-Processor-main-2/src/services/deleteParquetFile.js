const fs = require('fs');
const path = require('path');

const deleteParquetFiles = async () => {
  try {
    // Get current directory
    const currentDir = path.dirname(__filename);
    const parentDir = path.join(currentDir, '..', '..');
    
    // Find all parquet files in the directory
    const files = fs.readdirSync(parentDir).filter(file => file.endsWith('.parquet'));
    
    if (files.length === 0) {
      console.log('No parquet files found in the directory');
      return;
    }

    console.log(`Found ${files.length} parquet file(s) to delete`);
    
    // Delete each parquet file
    for (const file of files) {
      const filePath = path.join(parentDir, file);
      fs.unlinkSync(filePath);
      console.log(`Successfully deleted: ${file}`);
    }

    console.log('All parquet files have been deleted');
  } catch (error) {
    console.error(`Error deleting parquet files: ${error.message}`);
    throw error;
  }
};

module.exports = { deleteParquetFiles };