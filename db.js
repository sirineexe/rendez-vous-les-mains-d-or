/**
 * Database Module - Cabinet Dentaire Dr. Omar & Dr. Nesrine
 * Hybrid Architecture: PostgreSQL (Supabase Cloud) + SQLite (Local Dev)
 * 
 * DUAL-CHAIR RULE: 2 patients CAN have appointments at the SAME TIME
 * if they are with DIFFERENT doctors (Chair 1: Dr. Omar, Chair 2: Dr. Nesrine).
 */
const { Pool } = require('pg');
const path = require('node:path');
const fs = require('node:fs');

const isPostgres = Boolean(process.env.DATABASE_URL);

let pgPool = null;
let sqliteDb = null;

const DATA_DIR = path.join(__dirname, 'data');
const BACKUP_DIR = path.join(DATA_DIR, 'backups');
const DB_PATH = path.join(DATA_DIR, 'cabinet.db');

if (isPostgres) {
  console.log('[DB] Mode Cloud PostgreSQL (Supabase) activé');
  pgPool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });
} else {
  console.log('[DB] Mode Local SQLite WAL activé');
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive: true });
  
  const { DatabaseSync } = require('node:sqlite');
  sqliteDb = new DatabaseSync(DB_PATH);
  sqliteDb.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA busy_timeout = 5000;
    PRAGMA synchronous = NORMAL;
    PRAGMA foreign_keys = ON;
  `);
}

async function query(text, params = []) {
  if (isPostgres) {
    let paramIndex = 1;
    const pgText = text.replace(/\?/g, () => `$${paramIndex++}`);
    const res = await pgPool.query(pgText, params);
    return res.rows;
  } else {
    return sqliteDb.prepare(text).all(...params);
  }
}

async function queryOne(text, params = []) {
  if (isPostgres) {
    let paramIndex = 1;
    const pgText = text.replace(/\?/g, () => `$${paramIndex++}`);
    const res = await pgPool.query(pgText, params);
    return res.rows[0] || null;
  } else {
    return sqliteDb.prepare(text).get(...params) || null;
  }
}

async function execute(text, params = []) {
  if (isPostgres) {
    let paramIndex = 1;
    const pgText = text.replace(/\?/g, () => `$${paramIndex++}`);
    const res = await pgPool.query(pgText, params);
    return { lastInsertRowid: res.rows[0]?.id || null, rowCount: res.rowCount };
  } else {
    return sqliteDb.prepare(text).run(...params);
  }
}

async function initSchema() {
  if (isPostgres) {
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS settings (
        key VARCHAR(100) PRIMARY KEY,
        value TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS dentists (
        id SERIAL PRIMARY KEY,
        name VARCHAR(150) NOT NULL UNIQUE,
        speciality VARCHAR(150) DEFAULT 'Chirurgien-dentiste',
        color VARCHAR(20) NOT NULL DEFAULT '#2563eb',
        active INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS reasons (
        id SERIAL PRIMARY KEY,
        name VARCHAR(150) NOT NULL UNIQUE,
        default_duration INTEGER NOT NULL DEFAULT 30,
        color VARCHAR(20) DEFAULT '#3b82f6'
      );

      CREATE TABLE IF NOT EXISTS patients (
        id SERIAL PRIMARY KEY,
        nom VARCHAR(150) NOT NULL,
        prenom VARCHAR(150) NOT NULL,
        telephone VARCHAR(50) NOT NULL,
        email VARCHAR(150),
        date_naissance VARCHAR(50),
        remarques_medicales TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS appointments (
        id SERIAL PRIMARY KEY,
        date VARCHAR(20) NOT NULL,
        heure_debut VARCHAR(10) NOT NULL,
        heure_fin VARCHAR(10) NOT NULL,
        duree INTEGER NOT NULL,
        patient_nom VARCHAR(150) NOT NULL,
        patient_prenom VARCHAR(150) NOT NULL,
        patient_telephone VARCHAR(50) NOT NULL,
        patient_id INTEGER REFERENCES patients(id) ON DELETE SET NULL,
        motif VARCHAR(150) NOT NULL,
        dentiste_id INTEGER NOT NULL REFERENCES dentists(id) ON DELETE RESTRICT,
        statut VARCHAR(50) NOT NULL DEFAULT 'À confirmer',
        rappel_effectue INTEGER NOT NULL DEFAULT 0,
        rappel_date TIMESTAMP,
        remarque TEXT,
        created_by VARCHAR(100) DEFAULT 'Secrétariat',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_rdv_date ON appointments(date);
      CREATE INDEX IF NOT EXISTS idx_rdv_dentiste ON appointments(dentiste_id);
      CREATE INDEX IF NOT EXISTS idx_rdv_statut ON appointments(statut);

      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(150) NOT NULL UNIQUE,
        role VARCHAR(50) NOT NULL DEFAULT 'secretaire',
        pin VARCHAR(20) DEFAULT '0000',
        dentiste_id INTEGER REFERENCES dentists(id) ON DELETE SET NULL
      );

      CREATE TABLE IF NOT EXISTS audit_logs (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        operator VARCHAR(100) NOT NULL,
        action VARCHAR(100) NOT NULL,
        appointment_id INTEGER,
        details TEXT
      );
    `);

    const defaultSettings = [
      ['cabinet_name', 'Cabinet Dentaire Dr. Omar & Dr. Nesrine'],
      ['cabinet_phone', '0555 12 34 56'],
      ['opening_time', '08:00'],
      ['closing_time', '17:00'],
      ['slot_duration', '30'],
      ['first_day_of_week', 'Dimanche'],
      ['closed_day_1', 'Vendredi'],
      ['closed_day_2', ''],
      ['reminder_days_ahead', '1']
    ];

    for (const [key, val] of defaultSettings) {
      await pgPool.query(
        `INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
        [key, val]
      );
    }

    await pgPool.query(`
      INSERT INTO dentists (name, speciality, color, active) 
      VALUES ('Dr. Omar', 'Chirurgien-dentiste', '#2563eb', 1) 
      ON CONFLICT (name) DO NOTHING;
    `);
    await pgPool.query(`
      INSERT INTO dentists (name, speciality, color, active) 
      VALUES ('Dr. Nesrine', 'Chirurgien-dentiste', '#db2777', 1) 
      ON CONFLICT (name) DO NOTHING;
    `);

    await pgPool.query(`INSERT INTO users (name, role, pin) VALUES ('Secrétariat', 'secretaire', '1234') ON CONFLICT (name) DO NOTHING;`);
    await pgPool.query(`INSERT INTO users (name, role, pin) VALUES ('Administrateur', 'admin', '9999') ON CONFLICT (name) DO NOTHING;`);

    const reasonsCount = (await pgPool.query('SELECT COUNT(*) as c FROM reasons')).rows[0].c;
    if (Number(reasonsCount) === 0) {
      const defaultReasons = [
        ['Consultation', 30, '#3b82f6'],
        ['Contrôle', 30, '#0284c7'],
        ['Détartrage', 30, '#0d9488'],
        ['Carie / soin', 30, '#e11d48'],
        ['Dévitalisation (canal)', 60, '#7c3aed'],
        ['Extraction', 30, '#dc2626'],
        ['Prothèse (empreinte / pose)', 45, '#d97706'],
        ['Orthodontie', 30, '#9333ea'],
        ['Urgence', 30, '#ef4444'],
        ['Blanchiment', 60, '#06b6d4'],
        ['Radio', 15, '#64748b'],
        ['Autre', 30, '#6b7280']
      ];
      for (const [name, dur, color] of defaultReasons) {
        await pgPool.query('INSERT INTO reasons (name, default_duration, color) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING', [name, dur, color]);
      }
    }
  } else {
    // SQLite local
    sqliteDb.exec(`
      CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS dentists (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, speciality TEXT DEFAULT 'Chirurgien-dentiste', color TEXT NOT NULL DEFAULT '#2563eb', active INTEGER NOT NULL DEFAULT 1, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);
      CREATE TABLE IF NOT EXISTS reasons (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, default_duration INTEGER NOT NULL DEFAULT 30, color TEXT DEFAULT '#3b82f6');
      CREATE TABLE IF NOT EXISTS patients (id INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT NOT NULL, prenom TEXT NOT NULL, telephone TEXT NOT NULL, email TEXT, date_naissance TEXT, remarques_medicales TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP);
      CREATE TABLE IF NOT EXISTS appointments (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, heure_debut TEXT NOT NULL, heure_fin TEXT NOT NULL, duree INTEGER NOT NULL, patient_nom TEXT NOT NULL, patient_prenom TEXT NOT NULL, patient_telephone TEXT NOT NULL, patient_id INTEGER REFERENCES patients(id) ON DELETE SET NULL, motif TEXT NOT NULL, dentiste_id INTEGER NOT NULL REFERENCES dentists(id) ON DELETE RESTRICT, statut TEXT NOT NULL DEFAULT 'À confirmer', rappel_effectue INTEGER NOT NULL DEFAULT 0, rappel_date DATETIME, remarque TEXT, created_by TEXT DEFAULT 'Secrétariat', created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP);
      CREATE INDEX IF NOT EXISTS idx_rdv_date ON appointments(date);
      CREATE INDEX IF NOT EXISTS idx_rdv_dentiste ON appointments(dentiste_id);
      CREATE INDEX IF NOT EXISTS idx_rdv_statut ON appointments(statut);
      CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, role TEXT NOT NULL DEFAULT 'secretaire', pin TEXT DEFAULT '0000', dentiste_id INTEGER REFERENCES dentists(id) ON DELETE SET NULL);
      CREATE TABLE IF NOT EXISTS audit_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, operator TEXT NOT NULL, action TEXT NOT NULL, appointment_id INTEGER, details TEXT);
    `);

    const defaultSettings = [
      ['cabinet_name', 'Cabinet Dentaire Dr. Omar & Dr. Nesrine'],
      ['cabinet_phone', '0555 12 34 56'],
      ['opening_time', '08:00'],
      ['closing_time', '17:00'],
      ['slot_duration', '30'],
      ['first_day_of_week', 'Dimanche'],
      ['closed_day_1', 'Vendredi'],
      ['closed_day_2', ''],
      ['reminder_days_ahead', '1']
    ];
    const getSetting = sqliteDb.prepare('SELECT value FROM settings WHERE key = ?');
    const insertSetting = sqliteDb.prepare('INSERT INTO settings (key, value) VALUES (?, ?)');
    for (const [key, val] of defaultSettings) {
      if (!getSetting.get(key)) insertSetting.run(key, val);
    }

    const insertDentist = sqliteDb.prepare('INSERT OR IGNORE INTO dentists (name, speciality, color, active) VALUES (?, ?, ?, 1)');
    insertDentist.run('Dr. Omar', 'Chirurgien-dentiste', '#2563eb');
    insertDentist.run('Dr. Nesrine', 'Chirurgien-dentiste', '#db2777');

    const insertUser = sqliteDb.prepare('INSERT OR IGNORE INTO users (name, role, pin) VALUES (?, ?, ?)');
    insertUser.run('Secrétariat', 'secretaire', '1234');
    insertUser.run('Administrateur', 'admin', '9999');

    const countReasons = sqliteDb.prepare('SELECT COUNT(*) as c FROM reasons').get();
    if (countReasons.c === 0) {
      const insertReason = sqliteDb.prepare('INSERT INTO reasons (name, default_duration, color) VALUES (?, ?, ?)');
      const defaultReasons = [
        ['Consultation', 30, '#3b82f6'],
        ['Contrôle', 30, '#0284c7'],
        ['Détartrage', 30, '#0d9488'],
        ['Carie / soin', 30, '#e11d48'],
        ['Dévitalisation (canal)', 60, '#7c3aed'],
        ['Extraction', 30, '#dc2626'],
        ['Prothèse (empreinte / pose)', 45, '#d97706'],
        ['Orthodontie', 30, '#9333ea'],
        ['Urgence', 30, '#ef4444'],
        ['Blanchiment', 60, '#06b6d4'],
        ['Radio', 15, '#64748b'],
        ['Autre', 30, '#6b7280']
      ];
      for (const [name, dur, color] of defaultReasons) {
        insertReason.run(name, dur, color);
      }
    }
  }
}

function calculateEndTime(startTime, durationMinutes) {
  const [hours, mins] = startTime.split(':').map(Number);
  const totalMins = hours * 60 + mins + durationMinutes;
  const endHours = Math.floor(totalMins / 60) % 24;
  const endMins = totalMins % 60;
  return `${String(endHours).padStart(2, '0')}:${String(endMins).padStart(2, '0')}`;
}

function timeToMinutes(timeStr) {
  if (!timeStr) return 0;
  const [h, m] = timeStr.split(':').map(Number);
  return h * 60 + m;
}

async function checkAppointmentConflict(date, startTime, endTime, dentistId, excludeAppointmentId = null) {
  if (!dentistId) {
    throw new Error('Le choix du praticien (Dr. Omar ou Dr. Nesrine) est obligatoire.');
  }

  const startMin = timeToMinutes(startTime);
  const endMin = timeToMinutes(endTime);

  let sql = `
    SELECT a.*, d.name as dentiste_nom
    FROM appointments a
    JOIN dentists d ON a.dentiste_id = d.id
    WHERE a.date = ?
      AND a.dentiste_id = ?
      AND a.statut NOT IN ('Annulé', 'Reporté')
  `;
  const params = [date, Number(dentistId)];

  if (excludeAppointmentId) {
    sql += ` AND a.id != ?`;
    params.push(excludeAppointmentId);
  }

  const existingList = await query(sql, params);

  for (const rdv of existingList) {
    const rdvStart = timeToMinutes(rdv.heure_debut);
    const rdvEnd = timeToMinutes(rdv.heure_fin);
    if (startMin < rdvEnd && endMin > rdvStart) {
      return {
        hasConflict: true,
        conflictingWith: {
          id: rdv.id,
          patient: `${rdv.patient_nom} ${rdv.patient_prenom}`,
          heure_debut: rdv.heure_debut,
          heure_fin: rdv.heure_fin,
          motif: rdv.motif,
          dentiste: rdv.dentiste_nom
        }
      };
    }
  }

  return { hasConflict: false };
}

async function logAudit(operator, action, appointmentId, details) {
  try {
    const sql = `
      INSERT INTO audit_logs (operator, action, appointment_id, details)
      VALUES (?, ?, ?, ?)
    `;
    const detailStr = typeof details === 'object' ? JSON.stringify(details) : String(details || '');
    await execute(sql, [operator || 'Système', action, appointmentId || null, detailStr]);
  } catch (err) {
    console.error('Audit log error:', err);
  }
}

async function upsertPatient(nom, prenom, telephone) {
  if (!nom || !telephone) return null;
  const cleanNom = nom.trim();
  const cleanPrenom = (prenom || '').trim();
  const cleanTel = telephone.trim();

  const existing = await queryOne(`
    SELECT id FROM patients 
    WHERE (LOWER(nom) = LOWER(?) AND LOWER(prenom) = LOWER(?))
       OR telephone = ?
  `, [cleanNom, cleanPrenom, cleanTel]);

  if (existing) {
    await execute(`
      UPDATE patients 
      SET nom = ?, prenom = ?, telephone = ?, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `, [cleanNom, cleanPrenom, cleanTel, existing.id]);
    return existing.id;
  } else {
    if (isPostgres) {
      const res = await pgPool.query(
        `INSERT INTO patients (nom, prenom, telephone) VALUES ($1, $2, $3) RETURNING id`,
        [cleanNom, cleanPrenom, cleanTel]
      );
      return res.rows[0].id;
    } else {
      const res = sqliteDb.prepare(`
        INSERT INTO patients (nom, prenom, telephone) VALUES (?, ?, ?)
      `).run(cleanNom, cleanPrenom, cleanTel);
      return res.lastInsertRowid;
    }
  }
}

function checkpointWAL() {
  if (!isPostgres && sqliteDb) {
    try {
      sqliteDb.exec('PRAGMA wal_checkpoint(TRUNCATE);');
    } catch (e) {}
  }
}

module.exports = {
  isPostgres,
  query,
  queryOne,
  execute,
  pgPool,
  initSchema,
  calculateEndTime,
  timeToMinutes,
  checkAppointmentConflict,
  logAudit,
  upsertPatient,
  checkpointWAL,
  DB_PATH
};
