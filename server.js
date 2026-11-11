/**
 * Server Module - Cabinet Dentaire Dr. Omar & Dr. Nesrine
 * Hybrid Architecture: PostgreSQL (Supabase Cloud) + SQLite (Local Dev)
 * Dual-Chair Concurrency Engine with WebSocket real-time updates.
 */
const express = require('express');
const http = require('node:http');
const path = require('node:path');
const fs = require('node:fs');
const os = require('node:os');
const { spawn } = require('node:child_process');
const { WebSocketServer, WebSocket } = require('ws');
const {
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
} = require('./db');

process.on('SIGINT', () => {
  console.log('\nFermeture sécurisée du cabinet dentaire...');
  checkpointWAL();
  process.exit(0);
});
process.on('SIGTERM', () => {
  checkpointWAL();
  process.exit(0);
});

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

let publicCloudUrl = null;

function broadcast(type, payload = {}) {
  const message = JSON.stringify({ type, payload, timestamp: new Date().toISOString() });
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(message);
  });
}

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'CONNECTED',
    payload: { clientsCount: wss.clients.size, publicUrl: publicCloudUrl }
  }));
});

function getDayName(dayIndex) {
  return ['Dimanche', 'Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi'][dayIndex];
}

async function getSettingsMap() {
  const rows = await query('SELECT key, value FROM settings');
  const map = {};
  for (const r of rows) map[r.key] = r.value;
  return map;
}

function getLocalIPAddresses() {
  const interfaces = os.networkInterfaces();
  const addresses = [];
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) addresses.push(iface.address);
    }
  }
  return addresses;
}

// -------------------------------------------------------------
// APIS
// -------------------------------------------------------------
app.get('/api/public-url', (req, res) => {
  res.json({ success: true, publicUrl: publicCloudUrl || null, localIps: getLocalIPAddresses(), port: PORT });
});

app.get('/api/settings', async (req, res) => {
  try {
    const settings = await getSettingsMap();
    res.json({ success: true, settings });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/settings', async (req, res) => {
  try {
    const { settings, operator } = req.body;
    if (!settings || typeof settings !== 'object') {
      return res.status(400).json({ success: false, error: 'Données invalides' });
    }

    for (const [key, value] of Object.entries(settings)) {
      if (isPostgres) {
        await pgPool.query(
          `INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
          [key, String(value)]
        );
      } else {
        await execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', [key, String(value)]);
      }
    }

    await logAudit(operator || 'Admin', 'UPDATE_SETTINGS', null, 'Mise à jour des paramètres du cabinet');
    broadcast('SETTINGS_UPDATED', { settings });
    res.json({ success: true, message: 'Paramètres enregistrés avec succès' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/dentists', async (req, res) => {
  try {
    const dentists = await query('SELECT * FROM dentists ORDER BY active DESC, id ASC');
    res.json({ success: true, dentists });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/dentists', async (req, res) => {
  try {
    const { name, speciality, color, operator } = req.body;
    if (!name || !name.trim()) {
      return res.status(400).json({ success: false, error: 'Le nom du praticien est requis' });
    }
    const resDb = await execute(
      `INSERT INTO dentists (name, speciality, color, active) VALUES (?, ?, ?, 1)`,
      [name.trim(), (speciality || 'Chirurgien-dentiste').trim(), color || '#2563eb']
    );
    await logAudit(operator, 'CREATE_DENTIST', null, `Ajout du dentiste ${name}`);
    broadcast('DENTISTS_UPDATED');
    res.json({ success: true, id: resDb.lastInsertRowid });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.put('/api/dentists/:id', async (req, res) => {
  try {
    const { name, speciality, color, active, operator } = req.body;
    await execute(
      `UPDATE dentists SET name = ?, speciality = ?, color = ?, active = ? WHERE id = ?`,
      [name.trim(), speciality.trim(), color, active ? 1 : 0, req.params.id]
    );
    await logAudit(operator, 'UPDATE_DENTIST', null, `Modification du dentiste ${name}`);
    broadcast('DENTISTS_UPDATED');
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/reasons', async (req, res) => {
  try {
    const reasons = await query('SELECT * FROM reasons ORDER BY name ASC');
    res.json({ success: true, reasons });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/reasons', async (req, res) => {
  try {
    const { name, default_duration, color, operator } = req.body;
    if (!name || !name.trim()) {
      return res.status(400).json({ success: false, error: 'Le nom du motif est requis' });
    }
    const resDb = await execute(
      `INSERT INTO reasons (name, default_duration, color) VALUES (?, ?, ?)`,
      [name.trim(), Number(default_duration) || 30, color || '#3b82f6']
    );
    await logAudit(operator, 'CREATE_REASON', null, `Ajout du motif ${name}`);
    broadcast('REASONS_UPDATED');
    res.json({ success: true, id: resDb.lastInsertRowid });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/patients', async (req, res) => {
  try {
    const q = (req.query.q || '').trim();
    if (!q) {
      const patients = await query(`
        SELECT p.*, COUNT(a.id) as total_rdv
        FROM patients p
        LEFT JOIN appointments a ON a.patient_id = p.id
        GROUP BY p.id ORDER BY p.updated_at DESC LIMIT 50
      `);
      return res.json({ success: true, patients });
    }
    const pattern = `%${q}%`;
    const patients = await query(`
      SELECT p.*, COUNT(a.id) as total_rdv
      FROM patients p
      LEFT JOIN appointments a ON a.patient_id = p.id
      WHERE p.nom ILIKE ? OR p.prenom ILIKE ? OR p.telephone LIKE ?
      GROUP BY p.id ORDER BY p.nom ASC LIMIT 20
    `.replace(/ILIKE/g, isPostgres ? 'ILIKE' : 'LIKE'), [pattern, pattern, pattern]);
    res.json({ success: true, patients });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// -------------------------------------------------------------
// API: DISPONIBILITÉS (DUAL-CHAIR: DR. OMAR & DR. NESRINE)
// -------------------------------------------------------------
app.get('/api/disponibilites', async (req, res) => {
  try {
    const date = req.query.date || new Date().toISOString().split('T')[0];
    const dentistIdFilter = req.query.dentistId ? Number(req.query.dentistId) : null;

    const settings = await getSettingsMap();
    const openingTime = settings.opening_time || '08:00';
    const closingTime = settings.closing_time || '17:00';
    const slotDuration = Number(settings.slot_duration) || 30;
    const closedDay1 = (settings.closed_day_1 || '').trim();
    const closedDay2 = (settings.closed_day_2 || '').trim();

    const dateObj = new Date(date + 'T12:00:00');
    const dayOfWeek = getDayName(dateObj.getDay());
    const isClosed = (closedDay1 && dayOfWeek.toLowerCase() === closedDay1.toLowerCase()) ||
                     (closedDay2 && dayOfWeek.toLowerCase() === closedDay2.toLowerCase());

    const startMinutes = timeToMinutes(openingTime);
    const endMinutes = timeToMinutes(closingTime);
    const totalSlots = Math.floor((endMinutes - startMinutes) / slotDuration);

    let dentists = await query('SELECT * FROM dentists WHERE active = 1 ORDER BY id ASC');
    if (dentistIdFilter) {
      dentists = dentists.filter(d => d.id === dentistIdFilter);
    }

    const appointments = await query(`
      SELECT a.*, d.name as dentiste_nom, d.color as dentiste_color
      FROM appointments a
      JOIN dentists d ON a.dentiste_id = d.id
      WHERE a.date = ? AND a.statut NOT IN ('Annulé', 'Reporté')
      ORDER BY a.heure_debut ASC
    `, [date]);

    const slots = [];
    let totalFree = 0;
    let totalOccupied = 0;
    const doctorStats = {};
    dentists.forEach(d => {
      doctorStats[d.id] = { id: d.id, name: d.name, color: d.color, freeCount: 0, occupiedCount: 0, firstFree: null };
    });

    for (let i = 0; i < totalSlots; i++) {
      const slotStartMin = startMinutes + i * slotDuration;
      const slotEndMin = slotStartMin + slotDuration;
      const timeStr = `${String(Math.floor(slotStartMin / 60)).padStart(2, '0')}:${String(slotStartMin % 60).padStart(2, '0')}`;

      const slotChairs = [];

      for (const doctor of dentists) {
        if (isClosed) {
          slotChairs.push({ dentistId: doctor.id, dentistName: doctor.name, dentistColor: doctor.color, status: 'Fermé', appointment: null });
          continue;
        }

        const coveringRdv = appointments.find((rdv) => {
          if (Number(rdv.dentiste_id) !== Number(doctor.id)) return false;
          const rdvStart = timeToMinutes(rdv.heure_debut);
          const rdvEnd = timeToMinutes(rdv.heure_fin);
          return slotStartMin < rdvEnd && slotEndMin > rdvStart;
        });

        if (coveringRdv) {
          doctorStats[doctor.id].occupiedCount++;
          totalOccupied++;
          slotChairs.push({
            dentistId: doctor.id,
            dentistName: doctor.name,
            dentistColor: doctor.color,
            status: 'Occupé',
            appointment: {
              id: coveringRdv.id,
              patient: `${coveringRdv.patient_nom} ${coveringRdv.patient_prenom}`,
              telephone: coveringRdv.patient_telephone,
              motif: coveringRdv.motif,
              statut: coveringRdv.statut,
              dentiste: doctor.name,
              dentisteColor: doctor.color,
              isStartingSlot: coveringRdv.heure_debut === timeStr
            }
          });
        } else {
          doctorStats[doctor.id].freeCount++;
          totalFree++;
          if (!doctorStats[doctor.id].firstFree) doctorStats[doctor.id].firstFree = timeStr;
          slotChairs.push({ dentistId: doctor.id, dentistName: doctor.name, dentistColor: doctor.color, status: 'Libre', appointment: null });
        }
      }

      slots.push({ slotNumber: i + 1, time: timeStr, isClosed, chairs: slotChairs });
    }

    res.json({
      success: true, date, dayOfWeek, isClosed, totalSlots,
      totalChairs: dentists.length,
      totalSpots: totalSlots * dentists.length,
      totalFree, totalOccupied, doctorStats, dentists, slots
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// -------------------------------------------------------------
// API: APPOINTMENTS
// -------------------------------------------------------------
app.get('/api/appointments', async (req, res) => {
  try {
    const { date, startDate, endDate, dentistId, status, search } = req.query;
    let sql = `
      SELECT a.*, d.name as dentiste_nom, d.color as dentiste_color
      FROM appointments a
      LEFT JOIN dentists d ON a.dentiste_id = d.id
      WHERE 1=1
    `;
    const params = [];

    if (date) { sql += ` AND a.date = ?`; params.push(date); }
    else if (startDate && endDate) { sql += ` AND a.date BETWEEN ? AND ?`; params.push(startDate, endDate); }
    if (dentistId) { sql += ` AND a.dentiste_id = ?`; params.push(Number(dentistId)); }
    if (status) { sql += ` AND a.statut = ?`; params.push(status); }
    if (search) {
      sql += ` AND (a.patient_nom ILIKE ? OR a.patient_prenom ILIKE ? OR a.patient_telephone LIKE ? OR a.motif ILIKE ?)`.replace(/ILIKE/g, isPostgres ? 'ILIKE' : 'LIKE');
      const p = `%${search}%`;
      params.push(p, p, p, p);
    }
    sql += ` ORDER BY a.date ASC, a.heure_debut ASC`;
    const appointments = await query(sql, params);
    res.json({ success: true, appointments });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/appointments', async (req, res) => {
  try {
    const { date, heure_debut, duree, patient_nom, patient_prenom, patient_telephone, motif, dentiste_id, statut, remarque, operator } = req.body;

    if (!date || !heure_debut || !patient_nom || !patient_telephone || !motif || !dentiste_id) {
      return res.status(400).json({ success: false, error: 'Champs requis manquants (date, heure, nom, téléphone, motif, praticien).' });
    }

    const durationNum = Number(duree) || 30;
    const heure_fin = calculateEndTime(heure_debut, durationNum);
    const dentistIdNum = Number(dentiste_id);
    const operatorName = operator || 'Secrétariat';

    const conflictResult = await checkAppointmentConflict(date, heure_debut, heure_fin, dentistIdNum);
    if (conflictResult.hasConflict) {
      return res.status(409).json({
        success: false,
        error: `Conflit d'horaire ! ${conflictResult.conflictingWith.dentiste} a déjà un rendez-vous avec ${conflictResult.conflictingWith.patient} de ${conflictResult.conflictingWith.heure_debut} à ${conflictResult.conflictingWith.heure_fin}.`,
        conflictingWith: conflictResult.conflictingWith
      });
    }

    const patientId = await upsertPatient(patient_nom, patient_prenom, patient_telephone);

    let newId;
    if (isPostgres) {
      const insRes = await pgPool.query(`
        INSERT INTO appointments (date, heure_debut, heure_fin, duree, patient_nom, patient_prenom, patient_telephone, patient_id, motif, dentiste_id, statut, remarque, created_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING id
      `, [date, heure_debut, heure_fin, durationNum, patient_nom.trim(), (patient_prenom || '').trim(), patient_telephone.trim(), patientId, motif.trim(), dentistIdNum, statut || 'À confirmer', (remarque || '').trim(), operatorName]);
      newId = insRes.rows[0].id;
    } else {
      const insRes = await execute(`
        INSERT INTO appointments (date, heure_debut, heure_fin, duree, patient_nom, patient_prenom, patient_telephone, patient_id, motif, dentiste_id, statut, remarque, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [date, heure_debut, heure_fin, durationNum, patient_nom.trim(), (patient_prenom || '').trim(), patient_telephone.trim(), patientId, motif.trim(), dentistIdNum, statut || 'À confirmer', (remarque || '').trim(), operatorName]);
      newId = insRes.lastInsertRowid;
    }

    await logAudit(operatorName, 'CREATE_APPOINTMENT', newId, `Création RDV pour ${patient_nom} le ${date} à ${heure_debut}`);
    broadcast('APPOINTMENTS_UPDATED', { action: 'CREATE', date, appointmentId: newId, operator: operatorName });
    res.status(201).json({ success: true, message: 'Rendez-vous enregistré avec succès', id: newId });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.put('/api/appointments/:id', async (req, res) => {
  try {
    const id = Number(req.params.id);
    const { date, heure_debut, duree, patient_nom, patient_prenom, patient_telephone, motif, dentiste_id, statut, remarque, operator } = req.body;

    if (!date || !heure_debut || !patient_nom || !patient_telephone || !motif || !dentiste_id) {
      return res.status(400).json({ success: false, error: 'Champs requis manquants.' });
    }

    const durationNum = Number(duree) || 30;
    const heure_fin = calculateEndTime(heure_debut, durationNum);
    const dentistIdNum = Number(dentiste_id);
    const operatorName = operator || 'Secrétariat';

    if (statut !== 'Annulé' && statut !== 'Reporté') {
      const conflictResult = await checkAppointmentConflict(date, heure_debut, heure_fin, dentistIdNum, id);
      if (conflictResult.hasConflict) {
        return res.status(409).json({
          success: false,
          error: `Conflit d'horaire ! ${conflictResult.conflictingWith.dentiste} a déjà un rendez-vous avec ${conflictResult.conflictingWith.patient} de ${conflictResult.conflictingWith.heure_debut} à ${conflictResult.conflictingWith.heure_fin}.`,
          conflictingWith: conflictResult.conflictingWith
        });
      }
    }

    const patientId = await upsertPatient(patient_nom, patient_prenom, patient_telephone);
    await execute(`
      UPDATE appointments SET
        date=?, heure_debut=?, heure_fin=?, duree=?,
        patient_nom=?, patient_prenom=?, patient_telephone=?, patient_id=?,
        motif=?, dentiste_id=?, statut=?, remarque=?, updated_at=CURRENT_TIMESTAMP
      WHERE id=?
    `, [date, heure_debut, heure_fin, durationNum, patient_nom.trim(), (patient_prenom || '').trim(), patient_telephone.trim(), patientId, motif.trim(), dentistIdNum, statut, (remarque || '').trim(), id]);

    await logAudit(operatorName, 'UPDATE_APPOINTMENT', id, `Modification RDV le ${date} à ${heure_debut}`);
    broadcast('APPOINTMENTS_UPDATED', { action: 'UPDATE', date, appointmentId: id, operator: operatorName });
    res.json({ success: true, message: 'Rendez-vous mis à jour' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.patch('/api/appointments/:id/status', async (req, res) => {
  try {
    const id = Number(req.params.id);
    const { statut, operator } = req.body;
    const operatorName = operator || 'Secrétariat';

    const current = await queryOne('SELECT date, patient_nom FROM appointments WHERE id = ?', [id]);
    if (!current) return res.status(404).json({ success: false, error: 'Rendez-vous introuvable' });

    await execute('UPDATE appointments SET statut = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?', [statut, id]);
    await logAudit(operatorName, 'STATUS_CHANGE', id, `Statut passé à "${statut}" pour ${current.patient_nom}`);
    broadcast('APPOINTMENTS_UPDATED', { action: 'STATUS_CHANGE', date: current.date, appointmentId: id, newStatus: statut, operator: operatorName });
    res.json({ success: true, message: `Statut mis à jour : ${statut}` });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.patch('/api/appointments/:id/rappel', async (req, res) => {
  try {
    const id = Number(req.params.id);
    const { rappel_effectue, operator } = req.body;
    const operatorName = operator || 'Secrétariat';

    const current = await queryOne('SELECT date, patient_nom FROM appointments WHERE id = ?', [id]);
    if (!current) return res.status(404).json({ success: false, error: 'Rendez-vous introuvable' });

    const flag = rappel_effectue ? 1 : 0;
    await execute(`
      UPDATE appointments 
      SET rappel_effectue = ?, rappel_date = ${flag ? 'CURRENT_TIMESTAMP' : 'NULL'}, updated_at = CURRENT_TIMESTAMP 
      WHERE id = ?
    `, [flag, id]);

    await logAudit(operatorName, 'REMINDER_TOGGLE', id, `Rappel ${flag ? 'effectué' : 'réinitialisé'} pour ${current.patient_nom}`);
    broadcast('APPOINTMENTS_UPDATED', { action: 'REMINDER_TOGGLE', date: current.date, appointmentId: id, rappel_effectue: flag, operator: operatorName });
    res.json({ success: true, rappel_effectue: flag });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.delete('/api/appointments/:id', async (req, res) => {
  try {
    const id = Number(req.params.id);
    const operatorName = req.query.operator || 'Secrétariat';

    const current = await queryOne('SELECT date, patient_nom, heure_debut FROM appointments WHERE id = ?', [id]);
    if (!current) return res.status(404).json({ success: false, error: 'Rendez-vous introuvable' });

    await execute('DELETE FROM appointments WHERE id = ?', [id]);
    await logAudit(operatorName, 'DELETE_APPOINTMENT', id, `Suppression RDV de ${current.patient_nom} (${current.date} à ${current.heure_debut})`);
    broadcast('APPOINTMENTS_UPDATED', { action: 'DELETE', date: current.date, appointmentId: id, operator: operatorName });
    res.json({ success: true, message: 'Rendez-vous supprimé' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/rappels', async (req, res) => {
  try {
    const settings = await getSettingsMap();
    const daysAhead = Number(settings.reminder_days_ahead) || 1;
    let targetDate = req.query.targetDate;
    if (!targetDate) {
      const d = new Date();
      d.setDate(d.getDate() + daysAhead);
      targetDate = d.toISOString().split('T')[0];
    }

    const rows = await query(`
      SELECT a.*, d.name as dentiste_nom FROM appointments a
      LEFT JOIN dentists d ON a.dentiste_id = d.id
      WHERE a.date = ? AND a.statut NOT IN ('Annulé', 'Reporté')
      ORDER BY a.heure_debut ASC
    `, [targetDate]);

    const clinicName = settings.cabinet_name || 'Cabinet Dentaire';
    const enriched = rows.map((r) => {
      let cleanPhone = (r.patient_telephone || '').replace(/[^0-9+]/g, '');
      if (cleanPhone.startsWith('0')) cleanPhone = '213' + cleanPhone.substring(1);
      const dentistText = r.dentiste_nom ? ` avec le ${r.dentiste_nom}` : '';
      const message = `Bonjour ${r.patient_prenom ? r.patient_prenom + ' ' : ''}${r.patient_nom},\n\nLe ${clinicName} vous rappelle votre rendez-vous prévu le ${r.date} à ${r.heure_debut}${dentistText} (Motif : ${r.motif}).\n\nMerci de nous confirmer votre présence ou de nous prévenir au plus vite en cas d'empêchement.\nCordialement.`;
      return { ...r, cleanPhone, whatsappUrl: `https://wa.me/${cleanPhone}?text=${encodeURIComponent(message)}` };
    });

    res.json({ success: true, targetDate, daysAhead, count: enriched.length, patients: enriched });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/stats/monthly', async (req, res) => {
  try {
    const yearMonth = req.query.month || new Date().toISOString().substring(0, 7);
    const pattern = `${yearMonth}-%`;

    const totalPrevus = Number((await queryOne(`SELECT COUNT(*) as c FROM appointments WHERE date LIKE ? AND statut NOT IN ('Annulé', 'Reporté')`, [pattern])).c);
    const venus = Number((await queryOne(`SELECT COUNT(*) as c FROM appointments WHERE date LIKE ? AND statut = 'Venu'`, [pattern])).c);
    const absents = Number((await queryOne(`SELECT COUNT(*) as c FROM appointments WHERE date LIKE ? AND statut = 'Absent'`, [pattern])).c);
    const annules = Number((await queryOne(`SELECT COUNT(*) as c FROM appointments WHERE date LIKE ? AND statut = 'Annulé'`, [pattern])).c);
    const reportes = Number((await queryOne(`SELECT COUNT(*) as c FROM appointments WHERE date LIKE ? AND statut = 'Reporté'`, [pattern])).c);
    const aConfirmer = Number((await queryOne(`SELECT COUNT(*) as c FROM appointments WHERE date LIKE ? AND statut = 'À confirmer'`, [pattern])).c);
    const confirms = Number((await queryOne(`SELECT COUNT(*) as c FROM appointments WHERE date LIKE ? AND statut = 'Confirmé'`, [pattern])).c);

    const denominateur = venus + absents;
    const tauxAbsence = denominateur > 0 ? ((absents / denominateur) * 100).toFixed(1) : '0.0';

    const daysCounts = await query(`
      SELECT date, COUNT(*) as count FROM appointments
      WHERE date LIKE ? AND statut NOT IN ('Annulé', 'Reporté') GROUP BY date
    `, [pattern]);
    const countsMap = {};
    for (const d of daysCounts) countsMap[d.date] = Number(d.count);

    res.json({
      success: true, month: yearMonth,
      stats: { totalPrevus, venus, absents, annules, reportes, aConfirmer, confirms, tauxAbsence: `${tauxAbsence}%` },
      countsMap
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/controles', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];

    const missingPhones = await query(`
      SELECT id, date, heure_debut, patient_nom, patient_prenom, motif FROM appointments
      WHERE (patient_telephone IS NULL OR TRIM(patient_telephone) = '')
        AND statut NOT IN ('Annulé', 'Reporté') ORDER BY date DESC LIMIT 20
    `);

    const overdueStatus = await query(`
      SELECT id, date, heure_debut, patient_nom, patient_prenom, patient_telephone, statut, motif
      FROM appointments WHERE date < ? AND statut IN ('À confirmer', 'Confirmé')
      ORDER BY date DESC LIMIT 30
    `, [today]);

    const allActive = await query(`
      SELECT a.*, d.name as dentiste_nom FROM appointments a
      LEFT JOIN dentists d ON a.dentiste_id = d.id
      WHERE a.statut NOT IN ('Annulé', 'Reporté') AND a.date >= ?
      ORDER BY a.date ASC, a.heure_debut ASC
    `, [today]);

    const detectedConflicts = [];
    for (let i = 0; i < allActive.length; i++) {
      for (let j = i + 1; j < allActive.length; j++) {
        const a1 = allActive[i], a2 = allActive[j];
        if (a1.date !== a2.date) break;
        const s1 = timeToMinutes(a1.heure_debut), e1 = timeToMinutes(a1.heure_fin);
        const s2 = timeToMinutes(a2.heure_debut), e2 = timeToMinutes(a2.heure_fin);
        if (s1 < e2 && e1 > s2 && a1.dentiste_id === a2.dentiste_id) {
          detectedConflicts.push({ a1, a2 });
        }
      }
    }

    res.json({
      success: true,
      summary: {
        missingPhonesCount: missingPhones.length,
        overdueStatusCount: overdueStatus.length,
        detectedConflictsCount: detectedConflicts.length,
        totalAlerts: missingPhones.length + overdueStatus.length + detectedConflicts.length
      },
      missingPhones, overdueStatus, detectedConflicts
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/audit-logs', async (req, res) => {
  try {
    const logs = await query('SELECT * FROM audit_logs ORDER BY id DESC LIMIT 100');
    res.json({ success: true, logs });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/export/csv', async (req, res) => {
  try {
    const rows = await query(`
      SELECT a.id, a.date, a.heure_debut, a.heure_fin, a.duree,
        a.patient_nom, a.patient_prenom, a.patient_telephone,
        a.motif, COALESCE(d.name, 'Cabinet') as dentiste,
        a.statut, CASE WHEN a.rappel_effectue = 1 THEN 'Oui' ELSE 'Non' END as rappele,
        a.remarque, a.created_by, a.created_at
      FROM appointments a
      LEFT JOIN dentists d ON a.dentiste_id = d.id
      ORDER BY a.date DESC, a.heure_debut ASC
    `);

    let csv = 'ID;Date;Debut;Fin;Duree(min);Nom;Prenom;Telephone;Motif;Dentiste;Statut;Rappele;Remarque;Cree_Par;Cree_Le\n';
    for (const r of rows) {
      csv += [
        r.id, r.date, r.heure_debut, r.heure_fin, r.duree,
        `"${(r.patient_nom || '').replace(/"/g, '""')}"`,
        `"${(r.patient_prenom || '').replace(/"/g, '""')}"`,
        `"${(r.patient_telephone || '').replace(/"/g, '""')}"`,
        `"${(r.motif || '').replace(/"/g, '""')}"`,
        `"${(r.dentiste || '').replace(/"/g, '""')}"`,
        r.statut, r.rappele,
        `"${(r.remarque || '').replace(/"/g, '""')}"`,
        `"${(r.created_by || '').replace(/"/g, '""')}"`,
        r.created_at
      ].join(';') + '\n';
    }

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="rendez-vous-${new Date().toISOString().split('T')[0]}.csv"`);
    res.send('\uFEFF' + csv);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// START
const PORT = process.env.PORT || 3000;

initSchema().then(() => {
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`=======================================================`);
    console.log(`  🦷 CABINET DENTAIRE Dr. Omar & Dr. Nesrine`);
    console.log(`=======================================================`);
    console.log(` ✅ Serveur démarré sur le port ${PORT}`);
    console.log(` 🌐 Base : ${isPostgres ? 'PostgreSQL (Cloud Supabase)' : 'SQLite WAL (Local)'}`);
    console.log(` 🪑 Système 2 fauteuils : Dr. Omar & Dr. Nesrine simultanés`);
    console.log(`=======================================================`);
  });
}).catch((err) => {
  console.error('Erreur initialisation serveur:', err);
});
