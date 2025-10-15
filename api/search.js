// Файл: api/search.js (Финальная, надежная версия)

import { AsyncDuckDB, ConsoleLogger } from '@duckdb/duckdb-wasm';
import path from 'path';
import { promises as fs } from 'fs'; // <-- ДОБАВЛЕНО: импортируем файловую систему Node.js

// --- НАСТРОЙКИ ---
const ROOT_FOLDER_ID = '1QXot2uayhesa6XHFoi3bVvrtCQojCvxG'; 
const INDEX_FILE_PATH = path.resolve('./data/drive_index.parquet');

// --- Глобальная переменная для DuckDB ---
let db = null;

async function initDB() {
    if (db) return db;

    const logger = new ConsoleLogger();
    const _db = new AsyncDuckDB(logger);
    await _db.instantiate();

    // --- НОВЫЙ, НАДЕЖНЫЙ СПОСОБ ЗАГРУЗКИ ФАЙЛА ---
    // 1. Сначала читаем файл в память сами, используя Node.js
    console.log(`Чтение файла из: ${INDEX_FILE_PATH}`);
    const fileBuffer = await fs.readFile(INDEX_FILE_PATH);
    console.log(`Файл успешно прочитан, размер: ${fileBuffer.byteLength} байт.`);

    // 2. Регистрируем этот буфер данных в DuckDB как виртуальный файл
    await _db.registerFileBuffer('drive_index.parquet', fileBuffer);
    console.log('Файл-буфер зарегистрирован в DuckDB.');
    // --------------------------------------------------
    
    const con = await _db.connect();

    // 3. Теперь создаем таблицу из этого ВИРТУАЛЬНОГО файла
    await con.query(`CREATE TABLE items AS SELECT * FROM 'drive_index.parquet';`);
    console.log('Таблица "items" успешно создана из виртуального Parquet файла.');

    await con.query(`
        CREATE TABLE breadcrumb_map AS 
        SELECT i, n, p 
        FROM items 
        WHERE m = 'application/vnd.google-apps.folder' OR i = '${ROOT_FOLDER_ID}';
    `);
    
    await con.close();

    db = _db;
    return db;
}


// --- Главная функция-обработчик запросов (остается без изменений) ---
export default async function handler(req, res) {
    try {
        const dbInstance = await initDB();
        const con = await dbInstance.connect();

        const { folderId, q: searchTerm } = req.query;
        let responseData = { items: [], breadcrumbs: [] };

        if (searchTerm) {
            const query = `
                SELECT i, n, m, p
                FROM items 
                WHERE lower(n) LIKE lower('%${searchTerm}%') 
                LIMIT 200;
            `;
            const results = await con.query(query);
            responseData.items = results.toArray().map(Object.fromEntries);

        } else {
            const currentFolderId = (folderId === 'ROOT' || !folderId) ? ROOT_FOLDER_ID : folderId;
            
            const itemsQuery = `
                SELECT i, n, m 
                FROM items 
                WHERE p = '${currentFolderId}' 
                ORDER BY (CASE WHEN m = 'application/vnd.google-apps.folder' THEN 0 ELSE 1 END), n;
            `;
            const itemsResult = await con.query(itemsQuery);
            responseData.items = itemsResult.toArray().map(Object.fromEntries);
            
            const breadcrumbs = [{ i: ROOT_FOLDER_ID, n: 'Главная' }];
            if (currentFolderId !== ROOT_FOLDER_ID) {
                let currentId = currentFolderId;
                while(currentId && currentId !== ROOT_FOLDER_ID) {
                    const parentQuery = `SELECT i, n, p FROM breadcrumb_map WHERE i = '${currentId}' LIMIT 1;`;
                    const parentResult = await con.query(parentQuery);
                    if (parentResult.numRows > 0) {
                        const parent = Object.fromEntries(parentResult.toArray()[0]);
                        breadcrumbs.splice(1, 0, { i: parent.i, n: parent.n });
                        currentId = parent.p;
                    } else {
                        break;
                    }
                }
            }
            responseData.breadcrumbs = breadcrumbs;
        }

        await con.close();

        res.status(200).json(responseData);

    } catch (error) {
        console.error('Критическая ошибка в API:', error);
        res.status(500).json({ error: 'Произошла ошибка на сервере.' });
    }
}