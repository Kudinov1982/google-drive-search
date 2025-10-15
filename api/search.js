// Файл: api/search.js

import { AsyncDuckDB, ConsoleLogger } from '@duckdb/duckdb-wasm';
import path from 'path';

// --- НАСТРОЙКИ ---
// ID корневой папки вашего архива в Google Диске
const ROOT_FOLDER_ID = '1QXot2uayhesa6XHFoi3bVvrtCQojCvxG'; 
const INDEX_FILE = path.resolve('./data/drive_index.parquet');

// --- Глобальная переменная для DuckDB ---
// Мы инициализируем ее один раз, чтобы при "теплых" запусках функции она была готова
let db = null;

async function initDB() {
    // Если база уже готова, ничего не делаем
    if (db) return db;

    // Инициализируем DuckDB в памяти
    const logger = new ConsoleLogger();
    const _db = new AsyncDuckDB(logger);
    await _db.instantiate();
    
    // Подключаемся к базе данных
    const con = await _db.connect();

    // Загружаем наш Parquet-файл как таблицу в памяти.
    // Это самая долгая операция, но она выполняется только при "холодном" старте функции.
    await con.run(`CREATE TABLE items AS SELECT * FROM '${INDEX_FILE}';`);
    
    console.log('Таблица "items" успешно создана из Parquet файла.');

    // Создаем карту для быстрого построения "хлебных крошек"
    await con.run(`
        CREATE TABLE breadcrumb_map AS 
        SELECT i, n, p 
        FROM items 
        WHERE m = 'application/vnd.google-apps.folder' OR i = '${ROOT_FOLDER_ID}';
    `);
    
    await con.close();

    db = _db;
    return db;
}


// --- Главная функция-обработчик запросов ---
// Vercel автоматически вызывает эту функцию при обращении к /api/search
export default async function handler(req, res) {
    try {
        // Инициализируем базу данных (если она еще не готова)
        const dbInstance = await initDB();
        const con = await dbInstance.connect();

        const { folderId, q: searchTerm } = req.query;
        let responseData = { items: [], breadcrumbs: [] };

        if (searchTerm) {
            // --- РЕЖИМ ПОИСКА ---
            const query = `
                SELECT i, n, m, p
                FROM items 
                WHERE lower(n) LIKE lower('%${searchTerm}%') 
                LIMIT 200;
            `;
            const results = await con.query(query);
            responseData.items = results.toArray().map(Object.fromEntries);
            // Для результатов поиска "хлебные крошки" не строим, чтобы не усложнять

        } else {
            // --- РЕЖИМ ПРОСМОТРА ПАПКИ ---
            const currentFolderId = (folderId === 'ROOT' || !folderId) ? ROOT_FOLDER_ID : folderId;
            
            // Запрос для получения содержимого текущей папки
            const itemsQuery = `
                SELECT i, n, m 
                FROM items 
                WHERE p = '${currentFolderId}' 
                ORDER BY (CASE WHEN m = 'application/vnd.google-apps.folder' THEN 0 ELSE 1 END), n;
            `;
            const itemsResult = await con.query(itemsQuery);
            responseData.items = itemsResult.toArray().map(Object.fromEntries);
            
            // Запрос для построения "хлебных крошек"
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

        // Отправляем успешный ответ в формате JSON
        res.status(200).json(responseData);

    } catch (error) {
        console.error('Критическая ошибка в API:', error);
        // Отправляем ответ с ошибкой
        res.status(500).json({ error: 'Произошла ошибка на сервере.' });
    }
}