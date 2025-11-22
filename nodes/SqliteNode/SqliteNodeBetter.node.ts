import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
		NodeConnectionType,
	NodeOperationError,
} from 'n8n-workflow';
import type { Database as BetterSqlite3Database } from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import Database from 'better-sqlite3';
const binaryPath = path.join(__dirname, '../../../native/node-v127-linux-musl-x64/better_sqlite3.node');

async function all(db: BetterSqlite3Database, query: string, args: any): Promise<any> {
	return new Promise((resolve, reject) => {
		// For SELECT queries, use db.all() to get all rows
		try {
			const rows = db.prepare(query).all(args);
			resolve(rows);
		} catch (error) {
			reject(error);
		}
	});
}

async function run(db: BetterSqlite3Database, query: string, args: any): Promise<any> {
	return new Promise((resolve, reject) => {
		// For INSERT, UPDATE, DELETE queries, use db.run()
		try {
			const result = db.prepare(query).run(args);
			resolve({
				changes: result.changes, // Number of rows affected
				last_id: result.lastInsertRowid // The last inserted row ID
			});
		} catch (error) {
			reject(error);
		}
	});
}

async function exec(db: BetterSqlite3Database, query: string): Promise<any> {
	return new Promise((resolve, reject) => {
		try {
			// For other SQL commands (like CREATE, DROP, etc.), use db.exec()
			db.exec(query);
			resolve({ message: 'Query executed successfully.' });
		} catch (error: any) {
			reject(error);
		}
	});
}
 

export class SqliteNodeBetter implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'SQLite Node (Better)',
		name: 'sqliteNodeBetter',
		icon: 'file:sqlite-icon.svg',
		group: ['transform'],
		version: 1,
		description: 'A node to perform query in a local sqlite database',
		defaults: {
			name: 'Sqlite Node (Better)',
		},
		inputs: [NodeConnectionType.Main] as NodeConnectionType[],
		outputs: [NodeConnectionType.Main] as NodeConnectionType[],
		properties: [
			// Node properties which the user gets displayed and
			// can change on the node.
			{
				displayName: 'Database Path',
				name: 'db_path',
				type: 'string',
				default: '',
				placeholder: '/path/to/database.sqlite',
				description: 'The path to the SQLite database file',
				required: true,
			},
			{
				displayName: 'Query Type',
				name: 'query_type',
				type: 'options',
				default: 'AUTO',
				noDataExpression: true,
				required: true,
				options: [
					{
						name: 'AUTO',
						value: 'AUTO',
						description: 'Automatically detect the query type',
					},
					{
						name: 'CREATE',
						value: 'CREATE',
						description: 'Create a table',
					},
					{
						name: 'DELETE',
						value: 'DELETE',
						description: 'Delete rows from a table',
					},
					{
						name: 'INSERT',
						value: 'INSERT',
						description: 'Insert rows into a table',
					},
					{
						name: 'SELECT',
						value: 'SELECT',
						description: 'Select rows from a table (support for multiple queries)',
					},
					{
						name: 'UPDATE',
						value: 'UPDATE',
						description: 'Update rows in a table',
					},
				],
			},
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				default: '',
				placeholder: 'SELECT * FROM table where key = @key',
				description: 'The query to execute',
				required: true,
				typeOptions: {
					rows: 8,
				},
			},
			{
				displayName: 'Variables',
				name: 'formVariables',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
				},
				placeholder: 'Add Variable',
				default: {},
				description: 'Configure variables for the query using a form (easier than writing JSON)',
				options: [
					{
						displayName: 'Variable',
						name: 'variable',
						values: [
							{
								displayName: 'Name',
								name: 'name',
								type: 'string',
								default: '',
								placeholder: 'key',
								description: 'Variable name (without @ or $ prefix)',
								required: true,
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
								placeholder: 'value',
								description: 'Variable value',
								required: true,
							},
						],
					},
				],
			},
			{
				displayName: 'Args (JSON)',
				name: 'args',
				type: 'json',
				default: '{}',
				placeholder: '{"key": "value"}',
				description: 'The args that get passed to the query (JSON format, will be merged with form variables)',
			},
			{
				displayName: 'Spread Result',
				name: 'spread',
				type: 'boolean',
				default: false,
				description: 'Whether the result should be spread into multiple items',
				displayOptions: {
					show: {
						query_type: [
							'SELECT',
						],
					},
				},				
			},
			{
				displayName: 'Array Field Name',
				name: 'arrayFieldName',
				type: 'string',
				default: '',
				placeholder: 'results',
				description: 'If specified, returns a single item with an array field named by this value. If empty, returns items structure (each row as an item).',
				displayOptions: {
					show: {
						query_type: [
							'SELECT',
						],
					},
				},
			},
			{
				displayName: 'Additional Options',
				name: 'additionalOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Use Default Bindings',
						name: 'use_default_bindings',
						type: 'boolean',
						default: false,
						description: 'Whether you are running this outside of docker image and you want to use the default bindings for better-sqlite3',
					},
					{
						displayName: 'Use Custom Bindings',
						name: 'use_custom_bindings',
						type: 'string',
						default: binaryPath,
						description: 'Whether you want to provide your own better-sqlite3 bindings',
					},
				],
			}
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> 
	{
		const items = this.getInputData();

		let outputItems = [];
		for(let itemIndex = 0; itemIndex < items.length; itemIndex++) 
		{
			
			let db_path = this.getNodeParameter('db_path', itemIndex, '') as string;
			let query = this.getNodeParameter('query', itemIndex, '') as string;
			let args_string = this.getNodeParameter('args', itemIndex, '{}') as string;
			let query_type = this.getNodeParameter('query_type', itemIndex, '') as string;
			let arrayFieldName = this.getNodeParameter('arrayFieldName', itemIndex, '') as string;
			
			// Get form-based variables
			const formVariablesData = this.getNodeParameter('formVariables', itemIndex, {}) as {
				variable?: Array<{
					name: string;
					value: string;
				}>;
			};

			const additional_options = this.getNodeParameter('additionalOptions', 0, {}) as {
				use_default_bindings?: boolean;
				use_custom_bindings?: string;
			};
			

			const use_default_bindings = additional_options.use_default_bindings ?? false;
			const use_custom_bindings = additional_options.use_custom_bindings;

			if(query_type === 'AUTO') 
			{
				if(query.trim().toUpperCase().includes('SELECT')) 
					query_type = 'SELECT';
				else if(query.trim().toUpperCase().includes('INSERT')) 
					query_type = 'INSERT';
				else if(query.trim().toUpperCase().includes('UPDATE')) 
					query_type = 'UPDATE';
				else if(query.trim().toUpperCase().includes('DELETE')) 
					query_type = 'DELETE';
				else if(query.trim().toUpperCase().includes('CREATE')) 
					query_type = 'CREATE';
				else 
					query_type = 'AUTO';
			}

			if(db_path === '') 
				throw new NodeOperationError(this.getNode(), 'No database path provided.');
			

			if(query === '') 
				throw new NodeOperationError(this.getNode(), 'No query provided.');

			query = query.replace(/\$/g, '@'); // Replace $ with @ for better-sqlite3 compatibility

			let bindings: Database.Options = {
				nativeBinding: binaryPath,
			}
			if(use_default_bindings) {
				bindings = {};
			}
			if(use_custom_bindings) {
				bindings.nativeBinding = use_custom_bindings;
				if(fs.existsSync(use_custom_bindings)) {
					bindings.nativeBinding = use_custom_bindings;
				} else {
					throw new NodeOperationError(this.getNode(), `Custom bindings file not found at ${use_custom_bindings}`);
				}
			}

			const db = new Database(db_path, bindings);
			try 
			{
				// Parse JSON args
				let argsT: Record<string, any> = {};
				try {
					argsT = JSON.parse(args_string || '{}');
				} catch (e) {
					// If JSON parsing fails, use empty object
					argsT = {};
				}
				
				// Start with form variables
				let args: Record<string, any> = {};
				if (formVariablesData && formVariablesData.variable && Array.isArray(formVariablesData.variable)) {
					formVariablesData.variable.forEach((variable: { name: string; value: string }) => {
						if (variable.name && variable.name.trim() !== '') {
							// Remove @ or $ prefix if present
							const cleanName = variable.name.replace(/^[@$]/, '').trim();
							if (cleanName) {
								args[cleanName] = variable.value;
							}
						}
					});
				}
				
				// Merge with JSON args (JSON args take precedence if there are conflicts)
				for(const key in argsT)
				{
					const cleanKey = key.replace(/^[@$]/, '').trim();
					if (cleanKey) {
						args[cleanKey] = argsT[key];
					}
				}

				let results;
				if(query_type === 'SELECT') 
				{
					// if query contains multiple queries, split them and execute them one by one
					let queries = query.split(';').filter(q => q.trim() !== '');
					if(queries.length > 1)
					{
						results = await Promise.all(queries.map(async (q) => 
						{
							const query_args = { ...args };
							for(const key in query_args) 
							{
								if(!q.includes(key)) 
									delete query_args[key];
							}

							// For SELECT queries, use db.all() to get all rows
							return all(db, q, query_args);
						}));
					} 
					else 
					{
						const query_args = { ...args };
						for(const key in query_args) 
						{
							if(!query.includes(key)) 
								delete query_args[key];
						}

						// For SELECT queries, use db.all() to get all rows
						results = await all(db, query, query_args);
					}
				} 
				else if(['INSERT', 'UPDATE', 'DELETE'].includes(query_type)) 
				{
					const query_args = { ...args };
					for(const key in query_args) 
					{
						if(!query.includes(key)) 
							delete query_args[key];
					}

					// For INSERT, UPDATE, DELETE queries, use db.run() 
					results = await run(db, query, query_args)
				} 
				else 
				{
					const query_args = { ...args };
					for(const key in query_args) 
					{
						if(!query.includes(key)) 
							delete query_args[key];
					}

					// For other SQL commands (like CREATE, DROP, etc.), use db.run()
					results = await exec(db, query)
				}

				if(query_type === 'SELECT') 
				{
					// Handle multiple queries result
					if(Array.isArray(results) && results.length > 0 && Array.isArray(results[0])) 
					{
						// Multiple queries case
						if(arrayFieldName && arrayFieldName.trim() !== '') 
						{
							// Return single item with array field
							const resultObj: Record<string, any> = {};
							resultObj[arrayFieldName] = results;
							outputItems.push({json: resultObj});
						} 
						else 
						{
							// Return items structure - flatten all results
							results.forEach((resultArray: any[]) => {
								if(Array.isArray(resultArray)) {
									resultArray.forEach((row: any) => {
										outputItems.push({json: row});
									});
								} else {
									outputItems.push({json: resultArray});
								}
							});
						}
					} 
					else 
					{
						// Single query case
						if(arrayFieldName && arrayFieldName.trim() !== '') 
						{
							// Return single item with array field
							const resultObj: Record<string, any> = {};
							resultObj[arrayFieldName] = Array.isArray(results) ? results : [results];
							outputItems.push({json: resultObj});
						} 
						else 
						{
							// Return items structure (each row as an item) - default behavior
							const resultArray = Array.isArray(results) ? results : [results];
							resultArray.forEach((row: any) => {
								outputItems.push({json: row});
							});
						}
					}
				} 
				else 
				{
					outputItems.push({json: results});
				}
			} 
			catch(error) 
			{
				if(this.continueOnFail()) 
				{
					outputItems.push({
						json: {
							error: (error as Error).message || 'Unknown error',
						},
						pairedItem: {
							item: itemIndex,
						},
					});
				} 
				else 
				{
					// Adding `itemIndex` allows other workflows to handle this error
					if(error.context) 
					{
						// If the error thrown already contains the context property,
						// only append the itemIndex
						error.context.itemIndex = itemIndex;
						throw error;
					}

					throw new NodeOperationError(this.getNode(), error, {
						itemIndex,
						message: error.message,
					});
				}
			}
			finally 
			{
				db.close();
			}
		}

		return this.prepareOutputData(outputItems);
	}
}