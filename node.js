const { Pool } = require('pg');
const csv = require('csv-parser');
const fs = require('fs');

// Create a new connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

// Read data from CSV file and insert into PostgreSQL table
fs.createReadStream('bank_branches.csv')
  .pipe(csv())
  .on('data', async (row) => {
    try {
      // Create the table (if not already exists)
      await pool.query(`
        CREATE TABLE IF NOT EXISTS bank_branches (
          ifsc TEXT,
          bank_id TEXT,
          branch TEXT,
          address TEXT,
          city TEXT,
          district TEXT,
          state TEXT,
          bank_name TEXT
        );
      `);
      
      // Insert the row into the table
      await pool.query(`
        INSERT INTO bank_branches (ifsc, bank_id, branch, address, city, district, state, bank_name)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `, [row.ifsc, row.bank_id, row.branch, row.address, row.city, row.district, row.state, row.bank_name]);
      
      console.log(`Inserted row with IFSC code: ${row.ifsc}`);
    } catch (error) {
      console.error(`Error inserting row with IFSC code ${row.ifsc}: ${error.message}`);
    }
  })
  .on('end', () => {
    console.log('Data import completed');
    
    // Release the database connection pool
    pool.end();
  });
