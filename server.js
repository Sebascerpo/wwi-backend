require('dotenv').config();
const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Configuración mejorada del pool de conexiones
const pool = mysql.createPool({
    host: process.env.DB_HOST || '157.253.236.116',
    port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 8080,
    user: process.env.DB_USER || 'T_202514_js_cervantes',
    password: process.env.DB_PASS || '202411287',
    database: process.env.DB_NAME || 'T_202514_js_cervantes',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    acquireTimeout: 60000,
    timeout: 60000
});

// Helper para normalizar números
const normalizeNumber = (value) => {
    if (value === null || value === undefined) return 0;
    const num = Number(value);
    return Number.isNaN(num) ? 0 : num;
};

// --- HEALTH CHECK ---
app.get('/api/health', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT 1+1 AS test');
        res.json({ 
            status: 'ok', 
            database: rows[0].test === 2 ? 'connected' : 'error',
            timestamp: new Date().toISOString()
        });
    } catch (err) {
        res.status(500).json({ 
            status: 'error', 
            error: err.message,
            timestamp: new Date().toISOString()
        });
    }
});

// --- MOVIMIENTOS DETALLADOS ---
app.get('/api/movimientos', async (req, res) => {
    try {
        const { 
            fechaDesde, 
            fechaHasta, 
            cliente, 
            proveedor, 
            tipo, 
            limit = 1000, 
            offset = 0 
        } = req.query;

        const conditions = [];
        const params = [];

        // Filtros de fecha
        if (fechaDesde) { 
            conditions.push('f.Fecha >= ?'); 
            params.push(fechaDesde); 
        }
        if (fechaHasta) { 
            conditions.push('f.Fecha <= ?'); 
            params.push(fechaHasta); 
        }

        // Filtros por dimensiones
        if (cliente && cliente !== '0' && cliente !== 'null') { 
            conditions.push('m.ClienteDWH = ?'); 
            params.push(cliente); 
        }
        if (proveedor) { 
            conditions.push('p.NombreProveedor LIKE ?'); 
            params.push(`%${proveedor}%`); 
        }
        if (tipo) { 
            conditions.push('t.NombreTipoTransaccion LIKE ?'); 
            params.push(`%${tipo}%`); 
        }

        const whereClause = conditions.length ? ('WHERE ' + conditions.join(' AND ')) : '';

        const sql = `
            SELECT 
                f.Fecha,
                COALESCE(m.ClienteDWH, 0) AS ClienteID,
                CONCAT('Cliente ', COALESCE(m.ClienteDWH, 0)) AS ClienteNombre,
                p.NombreProveedor AS ProveedorNombre,
                t.NombreTipoTransaccion AS TipoTransaccion,
                m.Cantidad,
                m.ProductoDWH,
                m.FacturaID
            FROM FACT_Movimientos m
            JOIN DIM_Fecha f ON m.FechaDWH = f.FechaDWH
            JOIN DIM_Proveedor p ON m.ProveedorDWH = p.ProveedorDWH
            JOIN DIM_TipoTransaccion t ON m.TipoTransaccionDWH = t.TipoTransaccionDWH
            ${whereClause}
            ORDER BY f.Fecha DESC
            LIMIT ? OFFSET ?
        `;

        params.push(Number(limit));
        params.push(Number(offset));

        const [rows] = await pool.query(sql, params);

        // Obtener conteo total para paginación
        const countSql = `
            SELECT COUNT(*) AS total
            FROM FACT_Movimientos m
            JOIN DIM_Fecha f ON m.FechaDWH = f.FechaDWH
            JOIN DIM_Proveedor p ON m.ProveedorDWH = p.ProveedorDWH
            JOIN DIM_TipoTransaccion t ON m.TipoTransaccionDWH = t.TipoTransaccionDWH
            ${whereClause}
        `;
        
        const [countResult] = await pool.query(countSql, params.slice(0, -2));
        const totalRecords = countResult[0].total;

        res.json({ 
            success: true,
            data: rows.map(row => ({
                ...row,
                Cantidad: normalizeNumber(row.Cantidad)
            })),
            pagination: {
                total: Number(totalRecords),
                limit: Number(limit),
                offset: Number(offset),
                hasMore: (Number(offset) + rows.length) < Number(totalRecords)
            }
        });

    } catch (err) {
        console.error('Error en /api/movimientos:', err);
        res.status(500).json({ 
            success: false,
            error: err.message 
        });
    }
});

// --- RESUMEN Y KPIs ---
app.get('/api/summary', async (req, res) => {
    try {
        const { fechaDesde, fechaHasta, cliente, proveedor, tipo } = req.query;
        const conditions = [];
        const params = [];

        if (fechaDesde) { conditions.push('f.Fecha >= ?'); params.push(fechaDesde); }
        if (fechaHasta) { conditions.push('f.Fecha <= ?'); params.push(fechaHasta); }
        if (cliente && cliente !== '0' && cliente !== 'null') { 
            conditions.push('m.ClienteDWH = ?'); 
            params.push(cliente); 
        }
        if (proveedor) { 
            conditions.push('p.NombreProveedor LIKE ?'); 
            params.push(`%${proveedor}%`); 
        }
        if (tipo) { 
            conditions.push('t.NombreTipoTransaccion LIKE ?'); 
            params.push(`%${tipo}%`); 
        }

        const whereClause = conditions.length ? ('WHERE ' + conditions.join(' AND ')) : '';

        // KPIs principales
        const kpiSql = `
            SELECT
                COUNT(*) AS totalRegistros,
                COALESCE(SUM(ABS(m.Cantidad)), 0) AS totalMovimientos,
                COUNT(DISTINCT CASE 
                    WHEN m.ClienteDWH IS NOT NULL AND m.ClienteDWH > 0 AND m.ClienteDWH != 0
                    THEN m.ClienteDWH 
                    ELSE NULL 
                END) AS clientesActivos,
                COUNT(DISTINCT m.ProveedorDWH) AS proveedoresActivos
            FROM FACT_Movimientos m
            JOIN DIM_Fecha f ON m.FechaDWH = f.FechaDWH
            JOIN DIM_Proveedor p ON m.ProveedorDWH = p.ProveedorDWH
            JOIN DIM_TipoTransaccion t ON m.TipoTransaccionDWH = t.TipoTransaccionDWH
            ${whereClause}
        `;

        const [kpiRows] = await pool.query(kpiSql, params);
        const kpis = kpiRows[0];

        // Debug: verificar datos de clientes
        const debugClientesSql = `
            SELECT 
                COUNT(*) as total_registros,
                COUNT(DISTINCT m.ClienteDWH) as clientes_distintos,
                MIN(m.ClienteDWH) as min_cliente,
                MAX(m.ClienteDWH) as max_cliente,
                COUNT(CASE WHEN m.ClienteDWH IS NULL THEN 1 END) as clientes_null,
                COUNT(CASE WHEN m.ClienteDWH = 0 THEN 1 END) as clientes_cero
            FROM FACT_Movimientos m
            JOIN DIM_Fecha f ON m.FechaDWH = f.FechaDWH
            ${whereClause}
        `;
        const [debugClientes] = await pool.query(debugClientesSql, params);
        console.log('Debug clientes:', debugClientes[0]);

        // Top 15 proveedores
        const topProveedoresSql = `
            SELECT 
                p.NombreProveedor AS nombre,
                COALESCE(SUM(ABS(m.Cantidad)), 0) AS total
            FROM FACT_Movimientos m
            JOIN DIM_Fecha f ON m.FechaDWH = f.FechaDWH
            JOIN DIM_Proveedor p ON m.ProveedorDWH = p.ProveedorDWH
            JOIN DIM_TipoTransaccion t ON m.TipoTransaccionDWH = t.TipoTransaccionDWH
            ${whereClause}
            GROUP BY p.ProveedorDWH, p.NombreProveedor
            ORDER BY total DESC
            LIMIT 15
        `;
        const [topProveedores] = await pool.query(topProveedoresSql, params);

        // Top 15 clientes (excluyendo clienteID = 0 o NULL) - versión simplificada
        const topClientesSql = `
            SELECT 
                m.ClienteDWH AS clienteId,
                COALESCE(SUM(ABS(m.Cantidad)), 0) AS total
            FROM FACT_Movimientos m
            JOIN DIM_Fecha f ON m.FechaDWH = f.FechaDWH
            ${whereClause ? whereClause + ' AND' : 'WHERE'} 
            m.ClienteDWH IS NOT NULL AND m.ClienteDWH > 0
            GROUP BY m.ClienteDWH
            HAVING total > 0
            ORDER BY total DESC
            LIMIT 15
        `;
        const [topClientes] = await pool.query(topClientesSql, params);
        
        console.log('Top clientes query:', topClientesSql);
        console.log('Top clientes params:', params);
        console.log('Top clientes results:', topClientes);

        // Distribución por tipo de transacción
        const tiposSql = `
            SELECT 
                t.NombreTipoTransaccion AS tipo,
                COALESCE(SUM(ABS(m.Cantidad)), 0) AS total
            FROM FACT_Movimientos m
            JOIN DIM_Fecha f ON m.FechaDWH = f.FechaDWH
            JOIN DIM_Proveedor p ON m.ProveedorDWH = p.ProveedorDWH
            JOIN DIM_TipoTransaccion t ON m.TipoTransaccionDWH = t.TipoTransaccionDWH
            ${whereClause}
            GROUP BY t.TipoTransaccionDWH, t.NombreTipoTransaccion
            ORDER BY total DESC
        `;
        const [tiposTransaccion] = await pool.query(tiposSql, params);

        // Respuesta estructurada
        res.json({
            success: true,
            kpis: {
                totalRegistros: Number(kpis.totalRegistros || 0),
                totalMovimientos: normalizeNumber(kpis.totalMovimientos),
                clientesActivos: Number(kpis.clientesActivos || 0),
                proveedoresActivos: Number(kpis.proveedoresActivos || 0)
            },
            topProveedores: topProveedores.map(item => ({
                nombre: item.nombre,
                total: normalizeNumber(item.total)
            })),
            topClientes: topClientes.map(item => ({
                clienteId: item.clienteId,
                nombre: item.nombre,
                total: normalizeNumber(item.total)
            })),
            tiposTransaccion: tiposTransaccion.map(item => ({
                tipo: item.tipo,
                total: normalizeNumber(item.total)
            }))
        });

    } catch (err) {
        console.error('Error en /api/summary:', err);
        res.status(500).json({ 
            success: false,
            error: err.message 
        });
    }
});

// --- EVOLUCIÓN TEMPORAL (timeline) ---
app.get('/api/timeline', async (req, res) => {
  try {
    const { fechaDesde, fechaHasta, cliente, proveedor, tipo, granularity = 'month' } = req.query;

    const conditions = [];
    const params = [];

    // Filtros de fecha
    if (fechaDesde) { conditions.push('f.Fecha >= ?'); params.push(fechaDesde); }
    if (fechaHasta) { conditions.push('f.Fecha <= ?'); params.push(fechaHasta); }

    // Filtros opcionales
    if (cliente && cliente !== '0' && cliente !== 'null') { 
      conditions.push('m.ClienteDWH = ?'); 
      params.push(cliente); 
    }
    if (proveedor) { 
      conditions.push('p.NombreProveedor LIKE ?'); 
      params.push(`%${proveedor}%`); 
    }
    if (tipo) { 
      conditions.push('t.NombreTipoTransaccion LIKE ?'); 
      params.push(`%${tipo}%`); 
    }

    const whereClause = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';

    // Granularidad
    let dateSelect;
    switch (granularity) {
      case 'month':
        dateSelect = "DATE_FORMAT(f.Fecha, '%Y-%m-01')";
        break;
      case 'week':
        dateSelect = "DATE_SUB(f.Fecha, INTERVAL WEEKDAY(f.Fecha) DAY)";
        break;
      default:
        dateSelect = "DATE(f.Fecha)";
    }

    const sql = `
      SELECT 
        ${dateSelect} AS fecha,
        COALESCE(SUM(ABS(m.Cantidad)), 0) AS total,
        MIN(YEAR(f.Fecha)) AS anio,
        MIN(MONTH(f.Fecha)) AS mes
      FROM FACT_Movimientos m
      JOIN DIM_Fecha f ON m.FechaDWH = f.FechaDWH
      LEFT JOIN DIM_Proveedor p ON m.ProveedorDWH = p.ProveedorDWH
      LEFT JOIN DIM_TipoTransaccion t ON m.TipoTransaccionDWH = t.TipoTransaccionDWH
      ${whereClause}
      GROUP BY ${dateSelect}
      ORDER BY fecha
    `;

    // Logging para depuración
    console.log('SQL Timeline:', sql);
    console.log('Params Timeline:', params);

    const [rows] = await pool.query(sql, params);

    res.json({
      success: true,
      data: rows.map(r => ({
        fecha: r.fecha,
        total: normalizeNumber(r.total),
        anio: r.anio,
        mes: r.mes
      })),
      granularity
    });

  } catch (err) {
    console.error('Error en /api/timeline:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});
// --- OPCIONES PARA FILTROS ---
app.get('/api/options/:tipo', async (req, res) => {
    try {
        const { tipo } = req.params;
        let sql, field;

        switch (tipo) {
            case 'proveedores':
                sql = `
                    SELECT DISTINCT p.NombreProveedor AS value, p.NombreProveedor AS label
                    FROM DIM_Proveedor p
                    ORDER BY p.NombreProveedor
                    LIMIT 100
                `;
                break;
            case 'tipos':
                sql = `
                    SELECT DISTINCT t.NombreTipoTransaccion AS value, t.NombreTipoTransaccion AS label
                    FROM DIM_TipoTransaccion t
                    ORDER BY t.NombreTipoTransaccion
                    LIMIT 100
                `;
                break;
            case 'clientes':
                sql = `
                    SELECT DISTINCT 
                        m.ClienteDWH AS value, 
                        CONCAT('Cliente ', m.ClienteDWH) AS label
                    FROM FACT_Movimientos m
                    WHERE m.ClienteDWH IS NOT NULL AND m.ClienteDWH > 0
                    ORDER BY m.ClienteDWH
                    LIMIT 100
                `;
                break;
            default:
                return res.status(400).json({ 
                    success: false, 
                    error: 'Tipo no válido' 
                });
        }

        const [rows] = await pool.query(sql);
        
        res.json({
            success: true,
            data: rows
        });

    } catch (err) {
        console.error(`Error en /api/options/${req.params.tipo}:`, err);
        res.status(500).json({ 
            success: false,
            error: err.message 
        });
    }
});

// --- ERROR HANDLER ---
app.use((err, req, res, next) => {
    console.error('Error no manejado:', err);
    res.status(500).json({ 
        success: false,
        error: 'Error interno del servidor' 
    });
});

// --- 404 HANDLER ---
app.use((req, res) => {
    res.status(404).json({ 
        success: false,
        error: 'Endpoint no encontrado' 
    });
});

// --- START SERVER ---
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🚀 WWI API Server iniciado en puerto ${PORT}`);
    console.log(`📊 Health check: http://localhost:${PORT}/api/health`);
});