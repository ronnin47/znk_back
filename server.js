const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const bodyParser = require('body-parser');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const { toFile } = require('@imagekit/nodejs');
require('dotenv').config();



//********************************************************************************* */
// ==========================================
// IMAGEKIT
// ==========================================

const ImageKit = require('@imagekit/nodejs');

const imagekit = new ImageKit({
  privateKey: process.env.IMAGEKIT_PRIVATE_KEY
});

//listo a deploy
// ==========================================
// SUBIR IMAGEN A IMAGEKIT
// ==========================================

async function subirImagenImageKit(imagen, carpeta = 'general', nombreArchivo) {

  if (!imagen) {
    throw new Error('No se recibió ninguna imagen.');
  }

  const resultado = await imagekit.files.upload({
    file: imagen,
    fileName: nombreArchivo || `imagen_${Date.now()}.jpg`,
    folder: carpeta
  });

  if (!resultado.url || !resultado.fileId) {
    throw new Error('ImageKit no devolvió URL o fileId.');
  }

  return {
    imagenurl: resultado.url,
    imagencloudid: resultado.fileId
  };
}

//****************************************************** */



const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());

app.use(express.json({ limit: '50mb' })); // para parsear JSON
app.use(express.urlencoded({ extended: true, limit: '50mb' }));


// Servir la carpeta uploads como pública
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));



//SOCKETIO esta es la prueba para construir
const server = http.createServer(app);
// Inicializa el servidor de Socket.IO
const io = new Server(server, {
  cors: {
    origin: '*', // Cambiar por dominio exacto en producción
    methods: ['GET', 'POST'],
  },
});


console.trace("🚨 server.js ejecutado");


const connectedUsers = new Map();
const mensajesChat = [];

function normalizarMensaje(mensaje) {
  return {
    usuarioId: mensaje.usuarioId?.toString() ?? '',
    idpersonaje: mensaje.idpersonaje?.toString() ?? '',
    nombre: mensaje.nombre?.toString() ?? '',
    mensaje: mensaje.mensaje?.toString() ?? '',
    estatus: mensaje.estatus?.toString() ?? '',
    imagenurl: mensaje.imagenurl?.toString() ?? '',
    imagenPjUrl: mensaje.imagenPjUrl?.toString() ?? '',
    nick: mensaje.nick?.toString() ?? '',
    kenActual: mensaje.kenActual?.toString() ?? '',
    ken: mensaje.ken?.toString() ?? '',
    kiActual: mensaje.kiActual?.toString() ?? '',
    ki: mensaje.ki?.toString() ?? '',
    vidaActual: mensaje.vidaActual?.toString() ?? '',
    vidaTotal: mensaje.vidaTotal?.toString() ?? '',
    timestamp: Date.now(),
    tipo: mensaje.tipo?.toString() ?? '',
    idpersonajeReceptor:mensaje.idpersonajeReceptor?.toString() ?? '',
    idusuarioReceptor:mensaje.idusuarioReceptor?.toString() ?? '',
    nombreReceptor:mensaje.nombreReceptor?.toString() ?? '',
    puntajeKen:mensaje.puntajeKen?.toString() ?? '',
    nombreItem: mensaje.nombreItem?.toString() ?? '',
    idItem: mensaje.idItem ?? 0,
    idpersonajeDestino: mensaje.idpersonajeDestino?.toString() ?? '',
  };
}

io.on('connection', (socket) => {
  console.log(`Socket conectado: ${socket.id}`);

  // Eventos que ya tienes para la app móvil
  socket.on('user-connected', (userData) => {
    const { usuarioId, sesion } = userData;
    if (usuarioId && sesion) {
      connectedUsers.set(socket.id, usuarioId);
      console.log(`Usuario ${usuarioId} conectado.`);
      io.emit('connected-users', Array.from(connectedUsers.values()));
    }
  });

  socket.on('solicitar-historial', async () => {
    try {
      const resultado = await pool.query(
        `SELECT * FROM mensajes ORDER BY timestamp DESC LIMIT 80`
      );
      socket.emit('historial-chat', resultado.rows.reverse());
    } catch (error) {
      console.error('Error al cargar historial:', error);
      socket.emit('historial-chat', []);
    }
  });

socket.on('chat-chat', async (mensaje) => {
  if (mensaje.imagenBase64) {
    try {
      // 1. Preparar imagen para ImageKit
      const matches = mensaje.imagenBase64.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        console.error('❌ Imagen base64 inválida.');
        return;
      }

      const ext = matches[1];
      const data = matches[2];

      const nombreArchivo = `chat_${Date.now()}.${ext}`;

      const archivo = await toFile(
        Buffer.from(data, 'base64'),
        nombreArchivo
      );

      // 2. Subir imagen a ImageKit
      const resultado = await imagekit.files.upload({
        file: archivo,
        fileName: nombreArchivo,
        folder: '/chat-imagenes',
        useUniqueFileName: true
      });

      mensaje.mensaje = resultado.url;
      delete mensaje.imagenBase64;

      console.log('✅ Imagen subida a ImageKit:', resultado.url);

    } catch (error) {
      console.error('❌ Error al subir imagen a ImageKit:', error);
      return;
    }
  }

  const msgNormalizado = normalizarMensaje(mensaje);

  try {
    const insertQuery = `
      INSERT INTO mensajes (
        "usuarioId", idpersonaje, nombre, mensaje, estatus,
        imagenurl, "imagenPjUrl", nick,
        "kenActual", ken, "kiActual", ki,
        "vidaActual", "vidaTotal", timestamp, tipo,
        "idpersonajeReceptor", "idusuarioReceptor",
        "nombreReceptor", "puntajeKen"
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20
      )
      RETURNING id
    `;

    const { rows } = await pool.query(insertQuery, [
      msgNormalizado.usuarioId,
      msgNormalizado.idpersonaje,
      msgNormalizado.nombre,
      msgNormalizado.mensaje,
      msgNormalizado.estatus,
      msgNormalizado.imagenurl,
      msgNormalizado.imagenPjUrl,
      msgNormalizado.nick,
      msgNormalizado.kenActual,
      msgNormalizado.ken,
      msgNormalizado.kiActual,
      msgNormalizado.ki,
      msgNormalizado.vidaActual,
      msgNormalizado.vidaTotal,
      msgNormalizado.timestamp,
      msgNormalizado.tipo,
      msgNormalizado.idpersonajeReceptor,
      msgNormalizado.idusuarioReceptor,
      msgNormalizado.nombreReceptor,
      msgNormalizado.puntajeKen
    ]);

    msgNormalizado.id = rows[0].id;

  } catch (error) {
    console.error('❌ Error al guardar mensaje en DB:', error);
    msgNormalizado.id = Date.now().toString() + Math.random().toString(36).substring(2);
  }

  mensajesChat.push(msgNormalizado);

  if (mensajesChat.length > 80) {
    mensajesChat.shift();
  }

  io.emit('chat-chat', msgNormalizado);
});


   // ============================================================
  // EFECTO SOBRE PERSONAJE OBJETIVO
  // ============================================================

  socket.on('efecto-objetivo', (mensaje) => {
    console.log('EFECTO OBJETIVO RECIBIDO:', mensaje);

    io.emit('efecto-objetivo', mensaje);
  });

  // Eventos que usa la web
  socket.on('message', async (mensaje) => {
    const msgNormalizado = normalizarMensaje(mensaje);

    try {
      const insertQuery = `
        INSERT INTO mensajes (
          "usuarioId", idpersonaje, nombre, mensaje, estatus,
          imagenurl, "imagenPjUrl", nick,
          "kenActual", ken, "kiActual", ki,
          "vidaActual", "vidaTotal", timestamp, tipo
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        RETURNING id
      `;

      const { rows } = await pool.query(insertQuery, [
        msgNormalizado.usuarioId,
        msgNormalizado.idpersonaje,
        msgNormalizado.nombre,
        msgNormalizado.mensaje,
        msgNormalizado.estatus,
        msgNormalizado.imagenurl,
        msgNormalizado.imagenPjUrl,
        msgNormalizado.nick,
        msgNormalizado.kenActual,
        msgNormalizado.ken,
        msgNormalizado.kiActual,
        msgNormalizado.ki,
        msgNormalizado.vidaActual,
        msgNormalizado.vidaTotal,
        msgNormalizado.timestamp,
        msgNormalizado.tipo,
      ]);

      msgNormalizado.id = rows[0].id;
    } catch (error) {
      console.error('Error al guardar mensaje en DB:', error);
      msgNormalizado.id = Date.now().toString() + Math.random().toString(36).substring(2);
    }

    io.emit('message', msgNormalizado);
  });

  socket.on('image', (imageData) => {
    io.emit('image', imageData);
  });

  socket.on('removeImage', (idpersonaje) => {
    console.log(`Eliminando personaje con id: ${idpersonaje}`);
    io.emit('removeImage', idpersonaje);
  });

  socket.on('user-disconnect', (data) => {
    const { usuarioId } = data;
    const socketId = [...connectedUsers.entries()].find(([key, value]) => value === usuarioId)?.[0];
    if (socketId) {
      connectedUsers.delete(socketId);
      console.log(`Usuario ${usuarioId} se desconectó por cierre de sesión.`);
      io.emit('connected-users', Array.from(connectedUsers.values()));
    }
  });

  socket.on('disconnect', () => {
    const usuarioId = connectedUsers.get(socket.id);
    if (usuarioId) {
      connectedUsers.delete(socket.id);
      console.log(`Usuario ${usuarioId} se desconectó.`);
      io.emit('user-disconnect', { usuarioId });
      io.emit('connected-users', Array.from(connectedUsers.values()));
    }
  });
});


// BASE DE DATOS EN RENDER
const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: Number(process.env.DB_PORT),
  ssl: {
    rejectUnauthorized: false,
  },
});



app.get('/', (req, res) => {
  res.send('Servidor funcionando y conectado a PostgreSQL');
});


// Probar conexión al iniciar el servidor
pool.connect()
  .then(client => {
    return client
      .query('SELECT NOW()')
      .then(res => {
        console.log('✅ Conexión a la base de datos exitosa. Fecha actual:', res.rows[0].now);
        client.release();
      })
      .catch(err => {
        client.release();
        console.error('❌ Error al hacer la consulta inicial:', err.stack);
      });
  })
  .catch(err => {
    console.error('❌ No se pudo conectar a la base de datos:', err.stack);
  });

// Prueba conexión a la base de datos
app.get('/test-db', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW()'); // Consulta simple
    res.json({ now: result.rows[0].now });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});



//REGISTRO DE USUARIO -insert de usuario 
app.post('/insert-usuario', async (req, res) => {
  const { email, contrasenia,username } = req.body;
    const estatus="jugador"
    console.log("REQ: ",req.body)
  
  try {
    const query = `
      INSERT INTO usuarios (email, contrasenia, estatus, nick)
      VALUES ($1, $2, $3, $4)
      RETURNING idusuario
    `;

    const values = [email, contrasenia, estatus,username];
    const result = await pool.query(query, values);

    const newId = result.rows[0].idusuario;
    const newEstatus = result.rows[0].estatus;

    res.status(201).json({ message: `Bienvenido ${email}.`, idusuario: newId, estatus: newEstatus });
  } catch (err) {

    if (err.code === '23505') { 
      res.json({ message: 'El mail ya se encuentra registrado.' });
    } else {
      console.error('Error al insertar el usuario:', err.message);
      res.status(500).json({ error: 'Error al insertar el usuario.' });
    }

    
  }
});

//login 
app.post('/loginUsuario', async (req, res) => {
  const { email, contrasenia } = req.body;
  
  //console.log("",req.body)

  try {
    const userQuery = 'SELECT * FROM usuarios WHERE email = $1';
    const userResult = await pool.query(userQuery, [email]);

    if (userResult.rows.length === 0) {
      console.log("No se encontró el usuario con el email proporcionado.");
      return res.status(401).json({ message: 'Email o contraseña incorrectos' });
    }
 
    const user = userResult.rows[0];
    const idusuario = userResult.rows[0].idusuario;
    const estatus = userResult.rows[0].estatus;

    if (user.contrasenia !== contrasenia) {
      return res.status(401).json({ message: 'Email o contraseña incorrectos' });
    }
    res.json({
      message: 'Inicio de sesión exitoso',
      //personajes: personajesResult.rows, 
      idusuario: idusuario,
      estatus: estatus,
      user:user,
    });

  } catch (error) {
    console.error('Error en el inicio de sesión:', error);
    res.status(500).json({ message: 'Error en el servidor' });
  }
});


//Consumir personajes Usuario 
app.get('/consumirPersonajesUsuario', async (req, res) => {
  try {
    
    const { usuarioId } = req.query;
   // console.log("el id del usuario es: ",usuarioId)
    const userQuery = `
      SELECT 
        p.idpersonaje, p.nombre, p.dominio, p.raza, p.naturaleza, p.edad, p.ken, p.ki, p.destino, p."pDestino",
        p.fuerza, p.fortaleza, p.destreza, p.agilidad, p.sabiduria, p.presencia, p.principio,
        p.sentidos, p.academisismo, p.alerta, p.atletismo, p."conBakemono", p.mentir, p.pilotear,
        p."artesMarciales", p.medicina, p."conObjMagicos", p.sigilo, p."conEsferas", p."conLeyendas",
        p.forja, p."conDemonio", p."conEspiritual", p."manejoBlaster", p."manejoSombras", p."tratoBakemono",
        p."conHechiceria", p."medVital", p."medEspiritual", p.rayo, p.fuego, p.frio, p.veneno, p.corte,
        p.energia, p.ventajas, p."apCombate", p."valCombate", p."apCombate2", p."valCombate2",
        p.add1, p."valAdd1", p.add2, p."valAdd2", p.add3, p."valAdd3", p.add4, p."valAdd4",
        p.inventario, p.dominios, p."kenActual", p."kiActual", p.positiva, p.negativa, p."vidaActual",
        p.hechizos, p.consumision, p.iniciativa, p.historia, p."tecEspecial", p.conviccion, p.cicatriz,
        p.notasaga, p.resistencia, p."pjPnj", p.imagenurl, p.imagencloudid,p."imagenSeleccionada",p."coleccionImagenes",p.notas, p."usuarioId",
        
    a.spriteurl,
    a.filas,
    a.columnas,
    a.fps,
    a.scalesize,
    a.public_id 

  FROM personajes p
  LEFT JOIN animaciones a
    ON a.idpersonaje = p.idpersonaje

  WHERE p."usuarioId" = $1
  ORDER BY p.idpersonaje ASC
    `;
    const userResult = await pool.query(userQuery,[usuarioId]);

   
   if (userResult.rows.length === 0) {
  return res.status(200).json({
    message: 'Usuario sin personajes aún',
    coleccionPersonajes: [],  // ← importante
  });
}

    const coleccionPersonajes = userResult.rows;

   

    
    res.json({
      message: 'Peticion de personajes consumidos exitoso',
      coleccionPersonajes: coleccionPersonajes,   
    });

  } catch (error) {
    console.error('Error al obtener coleccion personajes del Usuario:', error);
    res.status(500).json({ message: 'Error en el servidor' });
  }
});


//Insert personaje

app.post('/insert-personaje', async (req, res) => {
  const {
    nombre, dominio, raza, naturaleza, edad, ken, ki, destino, pDestino,
    fuerza, fortaleza, destreza, agilidad, sabiduria, presencia, principio,
    sentidos, academisismo, alerta, atletismo, conBakemono, mentir, pilotear,
    artesMarciales, medicina, conObjMagicos, sigilo, conEsferas, conLeyendas,
    forja, conDemonio, conEspiritual, manejoBlaster, manejoSombras, tratoBakemono,
    conHechiceria, medVital, medEspiritual, rayo, fuego, frio, veneno, corte,
    energia, ventajas, apCombate, valCombate, apCombate2, valCombate2,
    add1, valAdd1, add2, valAdd2, add3, valAdd3, add4, valAdd4,
    imagen, inventario, dominios, kenActual, kiActual, positiva, negativa, vidaActual,
    hechizos, consumision, iniciativa, historia, usuarioId,
    tecEspecial, conviccion, cicatriz, notasaga, resistencia, pjPnj, notas
  } = req.body;

  console.log(`
===== INSERT PERSONAJE =====
Usuario: ${usuarioId}
Nombre: ${nombre}
`);

  try {
    // 1. Insertar personaje sin imagen
    const query = `
      INSERT INTO personajes (
        nombre, dominio, raza, naturaleza, edad, ken, ki, destino, "pDestino",
        fuerza, fortaleza, destreza, agilidad, sabiduria, presencia, principio,
        sentidos, academisismo, alerta, atletismo, "conBakemono", mentir, pilotear,
        "artesMarciales", medicina, "conObjMagicos", sigilo, "conEsferas", "conLeyendas",
        forja, "conDemonio", "conEspiritual", "manejoBlaster", "manejoSombras", "tratoBakemono",
        "conHechiceria", "medVital", "medEspiritual", rayo, fuego, frio, veneno, corte,
        energia, ventajas, "apCombate", "valCombate", "apCombate2", "valCombate2",
        add1, "valAdd1", add2, "valAdd2", add3, "valAdd3", add4, "valAdd4",
        inventario, dominios, "kenActual", "kiActual", positiva, negativa, "vidaActual",
        hechizos, consumision, iniciativa, historia, "tecEspecial", conviccion, cicatriz,
        notasaga, resistencia, "pjPnj", notas, "usuarioId"
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        $10, $11, $12, $13, $14, $15, $16,
        $17, $18, $19, $20, $21, $22, $23,
        $24, $25, $26, $27, $28, $29,
        $30, $31, $32, $33, $34, $35,
        $36, $37, $38, $39, $40, $41, $42, $43,
        $44, $45, $46, $47, $48, $49,
        $50, $51, $52, $53, $54, $55,
        $56, $57, $58, $59, $60, $61,
        $62, $63, $64, $65, $66, $67,
        $68, $69, $70, $71, $72, $73,
        $74, $75, $76
      )
      RETURNING idpersonaje
    `;

    const values = [
      nombre, dominio, raza, naturaleza, edad, ken, ki, destino, pDestino,
      fuerza, fortaleza, destreza, agilidad, sabiduria, presencia, principio,
      sentidos, academisismo, alerta, atletismo, conBakemono, mentir, pilotear,
      artesMarciales, medicina, conObjMagicos, sigilo, conEsferas, conLeyendas,
      forja, conDemonio, conEspiritual, manejoBlaster, manejoSombras, tratoBakemono,
      conHechiceria, medVital, medEspiritual, rayo, fuego, frio, veneno, corte,
      energia, ventajas, apCombate, valCombate, apCombate2, valCombate2,
      add1, valAdd1, add2, valAdd2, add3, valAdd3, add4, valAdd4,
      inventario, dominios, kenActual, kiActual, positiva, negativa, vidaActual,
      hechizos, consumision, iniciativa, historia, tecEspecial, conviccion, cicatriz,
      notasaga, resistencia, pjPnj, notas, usuarioId
    ];

    const result = await pool.query(query, values);
    const newId = result.rows[0].idpersonaje;

    let imageUrl = null;
    let imagencloudid = null;

    // 2. Subir imagen a ImageKit
    if (imagen) {
      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({ error: 'Imagen base64 inválida.' });
      }

      const ext = matches[1];
      const data = matches[2];

      const nombreArchivo = `personaje_${newId}.${ext}`;

      const archivo = await toFile(
        Buffer.from(data, 'base64'),
        nombreArchivo
      );

      const uploadResult = await imagekit.files.upload({
        file: archivo,
        fileName: nombreArchivo,
        folder: '/personajes',
        useUniqueFileName: false
      });

      imageUrl = uploadResult.url;
      imagencloudid = uploadResult.fileId;

      // 3. Actualizar URL y fileId en la base de datos
      await pool.query(
        `UPDATE personajes
         SET imagenurl = $1, imagencloudid = $2
         WHERE idpersonaje = $3`,
        [imageUrl, imagencloudid, newId]
      );
    }

    // 4. Responder con éxito
    res.status(201).json({
      message: 'Personaje creado exitosamente.',
      idpersonaje: newId,
      imagenurl: imageUrl,
      imagencloudid
    });

  } catch (err) {
    console.error('Error al insertar el personaje:', err);
    res.status(500).json({ error: 'Error al insertar el personaje.' });
  }
});


//Update personaje

app.put('/update-personaje/:id', async (req, res) => {
  const idpersonaje = req.params.id;

  let {
    nombre, dominio, raza, naturaleza, edad, ken, ki, destino, pDestino,
    fuerza, fortaleza, destreza, agilidad, sabiduria, presencia, principio,
    sentidos, academisismo, alerta, atletismo, conBakemono, mentir, pilotear,
    artesMarciales, medicina, conObjMagicos, sigilo, conEsferas, conLeyendas,
    forja, conDemonio, conEspiritual, manejoBlaster, manejoSombras, tratoBakemono,
    conHechiceria, medVital, medEspiritual, rayo, fuego, frio, veneno, corte,
    energia, ventajas, apCombate, valCombate, apCombate2, valCombate2,
    add1, valAdd1, add2, valAdd2, add3, valAdd3, add4, valAdd4,
    imagen, imagenurl, inventario, dominios, kenActual, kiActual,
    positiva, negativa, vidaActual, hechizos, consumision, iniciativa,
    historia, usuarioId, tecEspecial, conviccion, cicatriz, resistencia,
    imagenSeleccionada, pjPnj, notas
  } = req.body;

  resistencia = resistencia === null || resistencia === undefined || resistencia === ""
    ? 0
    : resistencia;

  console.log("=================================");
  console.log("ACTUALIZANDO PERSONAJE");
  console.log("ID personaje:", idpersonaje);
  console.log("Nombre:", nombre);
  console.log("=================================");

  console.log("Imagen seleccionada en el backend: ", imagenSeleccionada);

  try {
    let imagencloudid = null;

    // Subir imagen a ImageKit si llega en base64
    if (imagen && imagen.startsWith('data:image/')) {
      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({ error: 'Imagen base64 inválida.' });
      }

      const ext = matches[1];
      const data = matches[2];

      const nombreArchivo = `personaje_${idpersonaje}.${ext}`;

      const archivo = await toFile(
        Buffer.from(data, 'base64'),
        nombreArchivo
      );

      const uploadResult = await imagekit.files.upload({
        file: archivo,
        fileName: nombreArchivo,
        folder: '/personajes',
        useUniqueFileName: false
      });

      imagenurl = uploadResult.url;
      imagencloudid = uploadResult.fileId;
    }

    // UPDATE completo incluyendo imagenurl e imagencloudid
    const query = `
      UPDATE personajes SET
        nombre=$1, dominio=$2, raza=$3, naturaleza=$4, edad=$5,
        ken=$6, ki=$7, destino=$8, "pDestino"=$9, fuerza=$10,
        fortaleza=$11, destreza=$12, agilidad=$13, sabiduria=$14,
        presencia=$15, principio=$16, sentidos=$17, academisismo=$18,
        alerta=$19, atletismo=$20, "conBakemono"=$21, mentir=$22, pilotear=$23,
        "artesMarciales"=$24, medicina=$25, "conObjMagicos"=$26, sigilo=$27,
        "conEsferas"=$28, "conLeyendas"=$29, forja=$30, "conDemonio"=$31,
        "conEspiritual"=$32, "manejoBlaster"=$33, "manejoSombras"=$34,
        "tratoBakemono"=$35, "conHechiceria"=$36, "medVital"=$37, "medEspiritual"=$38,
        rayo=$39, fuego=$40, frio=$41, veneno=$42, corte=$43, energia=$44, ventajas=$45,
        "apCombate"=$46, "valCombate"=$47, "apCombate2"=$48, "valCombate2"=$49,
        add1=$50, "valAdd1"=$51, add2=$52, "valAdd2"=$53, add3=$54, "valAdd3"=$55,
        add4=$56, "valAdd4"=$57, inventario=$58, dominios=$59, "kenActual"=$60,
        "kiActual"=$61, positiva=$62, negativa=$63, "vidaActual"=$64,
        hechizos=$65, consumision=$66, iniciativa=$67, historia=$68,
        "usuarioId"=$69, "tecEspecial"=$70, conviccion=$71, cicatriz=$72,
        resistencia=$73, "pjPnj"=$74, imagenurl=$75,
        "imagenSeleccionada"=$76, notas=$77,
        imagencloudid=COALESCE($78, imagencloudid)
      WHERE idpersonaje=$79
    `;

    const values = [
      nombre, dominio, raza, naturaleza, edad, ken, ki, destino, pDestino,
      fuerza, fortaleza, destreza, agilidad, sabiduria, presencia, principio,
      sentidos, academisismo, alerta, atletismo, conBakemono, mentir, pilotear,
      artesMarciales, medicina, conObjMagicos, sigilo, conEsferas, conLeyendas,
      forja, conDemonio, conEspiritual, manejoBlaster, manejoSombras, tratoBakemono,
      conHechiceria, medVital, medEspiritual, rayo, fuego, frio, veneno, corte,
      energia, ventajas, apCombate, valCombate, apCombate2, valCombate2,
      add1, valAdd1, add2, valAdd2, add3, valAdd3, add4, valAdd4,
      inventario, dominios, kenActual, kiActual, positiva, negativa, vidaActual,
      hechizos, consumision, iniciativa, historia, usuarioId,
      tecEspecial, conviccion, cicatriz, resistencia, pjPnj,
      imagenurl, imagenSeleccionada, notas, imagencloudid, idpersonaje
    ];

    const nombresCampos = [
      "nombre", "dominio", "raza", "naturaleza", "edad",
      "ken", "ki", "destino", "pDestino", "fuerza",
      "fortaleza", "destreza", "agilidad", "sabiduria",
      "presencia", "principio", "sentidos", "academisismo",
      "alerta", "atletismo", "conBakemono", "mentir",
      "pilotear", "artesMarciales", "medicina",
      "conObjMagicos", "sigilo", "conEsferas",
      "conLeyendas", "forja", "conDemonio",
      "conEspiritual", "manejoBlaster",
      "manejoSombras", "tratoBakemono",
      "conHechiceria", "medVital", "medEspiritual",
      "rayo", "fuego", "frio", "veneno", "corte",
      "energia", "ventajas", "apCombate",
      "valCombate", "apCombate2", "valCombate2",
      "add1", "valAdd1", "add2", "valAdd2",
      "add3", "valAdd3", "add4", "valAdd4",
      "inventario", "dominios", "kenActual",
      "kiActual", "positiva", "negativa",
      "vidaActual", "hechizos", "consumision",
      "iniciativa", "historia", "usuarioId",
      "tecEspecial", "conviccion", "cicatriz",
      "resistencia", "pjPnj", "imagenurl",
      "imagenSeleccionada", "notas", "imagencloudid",
      "idpersonaje"
    ];

    values.forEach((valor, index) => {
      if (valor === "") {
        console.log("=================================");
        console.log("CAMPO VACIO DETECTADO:");
        console.log("Campo:", nombresCampos[index]);
        console.log("Posicion SQL:", index + 1);
        console.log("=================================");
      }
    });

    await pool.query(query, values);

    res.status(201).json({
      message: 'Personaje modificado exitosamente.',
      idpersonaje,
      imagenurl,
      imagencloudid
    });

  } catch (err) {
    console.error('Error al modificar el personaje:', err.message);
    res.status(500).json({ error: 'Error al modificar el personaje.' });
  }
});

//Delete personaje
app.delete('/deletePersonaje/:id', async (req, res) => {
  const idpersonaje = parseInt(req.params.id, 10);

  try {
    // 1) Obtener el imagencloudid (fileId de ImageKit)
    const { rows } = await pool.query(
      'SELECT imagencloudid FROM personajes WHERE idpersonaje = $1',
      [idpersonaje]
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: 'Personaje no encontrado.' });
    }

    const imagencloudid = rows[0].imagencloudid;

    // 2) Eliminar el personaje de la base de datos
    const result = await pool.query(
      'DELETE FROM personajes WHERE idpersonaje = $1 RETURNING *',
      [idpersonaje]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'No se pudo eliminar el personaje.' });
    }

    // 3) Si había imagencloudid, eliminar imagen de ImageKit
    if (imagencloudid) {
      try {
        await imagekit.files.delete(imagencloudid);
        console.log(`🗑️ Imagen ${imagencloudid} eliminada de ImageKit`);
      } catch (imageErr) {
        console.error('❌ Error al eliminar imagen en ImageKit:', imageErr.message);
      }
    }

    res.status(200).json({
      message: 'Personaje y su imagen eliminados correctamente.',
      deletedPersonaje: result.rows[0]
    });

  } catch (error) {
    console.error('🚨 Error al eliminar personaje:', error);
    res.status(500).json({ message: 'Error interno del servidor.' });
  }
});

//consumir los todos los personajes
app.get('/consumirPersonajesTodos', async (req, res) => {
  try {
    const userQuery = `
      SELECT 
        p.idpersonaje, p.nombre, p.dominio, p.raza, p.naturaleza, p.edad, p.ken, p.ki, p.destino, p."pDestino",
        p.fuerza, p.fortaleza, p.destreza, p.agilidad, p.sabiduria, p.presencia, p.principio,
        p.sentidos, p.academisismo, p.alerta, p.atletismo, p."conBakemono", p.mentir, p.pilotear,
        p."artesMarciales", p.medicina, p."conObjMagicos", p.sigilo, p."conEsferas", p."conLeyendas",
        p.forja, p."conDemonio", p."conEspiritual", p."manejoBlaster", p."manejoSombras", p."tratoBakemono",
        p."conHechiceria", p."medVital", p."medEspiritual", p.rayo, p.fuego, p.frio, p.veneno, p.corte,
        p.energia, p.ventajas, p."apCombate", p."valCombate", p."apCombate2", p."valCombate2",
        p.add1, p."valAdd1", p.add2, p."valAdd2",	p.add3,	p."valAdd3",	p.add4,	p."valAdd4",
        p.inventario, p.dominios, p."kenActual", p."kiActual", p.positiva, p.negativa, p."vidaActual",
        p.hechizos, p.consumision, p.iniciativa, p.historia, p."tecEspecial", p.conviccion, p.cicatriz,
        p.notasaga, p.resistencia, p."pjPnj", p.imagenurl,p.imagencloudid,p."imagenSeleccionada",p."coleccionImagenes", p."usuarioId", p.notas, a.spriteurl,
    a.filas,
    a.columnas,
    a.fps,
    a.scalesize,
    a.public_id 
        FROM personajes p
  LEFT JOIN animaciones a
    ON a.idpersonaje = p.idpersonaje
    `;



    const userResult = await pool.query(userQuery);

    if (userResult.rows.length === 0) {
  return res.status(404).json({ message: 'No se encontraron personajes en la base de datos' });
}

    const coleccionPersonajes = userResult.rows;
    res.json({
      message: 'Se consumiron todos los personajes',
      coleccionPersonajes: coleccionPersonajes,   
    });

  } catch (error) {
    console.error('Error al obtener coleccion todos los persoanjes de la base de datos:', error);
    res.status(500).json({ message: 'Error en el servidor' });
  }
});


//*****************MARCA PARA ESCRITORIO WPF************************************

app.get('/consumirUnicosWPF', async (req, res) => {
  try {
    const { idpersonaje } = req.query;

    if (!idpersonaje || isNaN(Number(idpersonaje))) {
      return res.status(400).json({
        message: 'Debe proporcionar un idpersonaje válido'
      });
    }

    const accionesQuery = `
      SELECT *
      FROM acciones
      WHERE idpersonaje = $1
        AND tipo IN ('Tecnica Especial', 'Especial')
    `;

    const itemsQuery = `
      SELECT *
      FROM items
      WHERE idpersonaje = $1
        AND rareza = 'Unico'
    `;

    const [accionesResult, itemsResult] = await Promise.all([
      pool.query(accionesQuery, [idpersonaje]),
      pool.query(itemsQuery, [idpersonaje])
    ]);

    // Separar las acciones por categoría
    const tecnicasEspeciales = accionesResult.rows.filter(
      accion => accion.tipo === 'Tecnica Especial'
    );

    const especiales = accionesResult.rows.filter(
      accion => accion.tipo === 'Especial'
    );

    const objetosUnicos = itemsResult.rows;

    // Mostrar en consola lo recuperado de PostgreSQL
    console.log('========== ESPECIALES WPF ==========');
    console.log('ID del personaje:', idpersonaje);

    console.log('Técnicas especiales encontradas:', tecnicasEspeciales.length);
    console.log(tecnicasEspeciales);

    console.log('Especiales encontrados:', especiales.length);
    console.log(especiales);

    console.log('Objetos únicos encontrados:', objetosUnicos.length);
    console.log(objetosUnicos);

    console.log('====================================');

    return res.json({
      message: 'Especiales obtenidos correctamente',
      tecnicasEspeciales: tecnicasEspeciales,
      especiales: especiales,
      objetosUnicos: objetosUnicos
    });

  } catch (error) {
    console.error(
      'Error al obtener técnicas especiales, especiales y objetos únicos:',
      error
    );

    return res.status(500).json({
      message: 'Error en el servidor'
    });
  }
});

//consumir los todos los personajes OK!!
app.get('/consumirPersonajesTodosWPF', async (req, res) => {
  try {
    const userQuery = `
      SELECT 
        p.idpersonaje, p.nombre, p.dominio, p.raza, p.naturaleza, p.edad, p.ken, p.ki, p.destino, p."pDestino",
        p.fuerza, p.fortaleza, p.destreza, p.agilidad, p.sabiduria, p.presencia, p.principio,
        p.sentidos, p.academisismo, p.alerta, p.atletismo, p."conBakemono", p.mentir, p.pilotear,
        p."artesMarciales", p.medicina, p."conObjMagicos", p.sigilo, p."conEsferas", p."conLeyendas",
        p.forja, p."conDemonio", p."conEspiritual", p."manejoBlaster", p."manejoSombras", p."tratoBakemono",
        p."conHechiceria", p."medVital", p."medEspiritual", p.rayo, p.fuego, p.frio, p.veneno, p.corte,
        p.energia, p.ventajas, p."apCombate", p."valCombate", p."apCombate2", p."valCombate2",
        p.add1, p."valAdd1", p.add2, p."valAdd2",	p.add3,	p."valAdd3",	p.add4,	p."valAdd4",
        p.inventario, p.dominios, p."kenActual", p."kiActual", p.positiva, p.negativa, p."vidaActual",
        p.hechizos, p.consumision, p.iniciativa, p.historia, p."tecEspecial", p.conviccion, p.cicatriz,
        p.notasaga, p.resistencia, p."pjPnj", p.imagenurl,p.imagencloudid,p."imagenSeleccionada",p."coleccionImagenes", p."usuarioId", p.notas,p.marca, a.spriteurl,
    a.filas,
    a.columnas,
    a.fps,
    a.scalesize,
    a.public_id 
        FROM personajes p
  LEFT JOIN animaciones a
    ON a.idpersonaje = p.idpersonaje
    `;



    const userResult = await pool.query(userQuery);

    if (userResult.rows.length === 0) {
  return res.status(404).json({ message: 'No se encontraron personajes en la base de datos' });
}

    const coleccionPersonajes = userResult.rows;
    res.json({
      message: 'Se consumiron todos los personajes',
      coleccionPersonajes: coleccionPersonajes,   
    });

  } catch (error) {
    console.error('Error al obtener coleccion todos los persoanjes de la base de datos:', error);
    res.status(500).json({ message: 'Error en el servidor' });
  }
});
//Consumir personajes Usuario  OK!!
app.get('/consumirPersonajesUsuarioWPF', async (req, res) => {
  try {
    
    const { usuarioId } = req.query;
   // console.log("el id del usuario es: ",usuarioId)
    const userQuery = `
      SELECT 
        p.idpersonaje, p.nombre, p.dominio, p.raza, p.naturaleza, p.edad, p.ken, p.ki, p.destino, p."pDestino",
        p.fuerza, p.fortaleza, p.destreza, p.agilidad, p.sabiduria, p.presencia, p.principio,
        p.sentidos, p.academisismo, p.alerta, p.atletismo, p."conBakemono", p.mentir, p.pilotear,
        p."artesMarciales", p.medicina, p."conObjMagicos", p.sigilo, p."conEsferas", p."conLeyendas",
        p.forja, p."conDemonio", p."conEspiritual", p."manejoBlaster", p."manejoSombras", p."tratoBakemono",
        p."conHechiceria", p."medVital", p."medEspiritual", p.rayo, p.fuego, p.frio, p.veneno, p.corte,
        p.energia, p.ventajas, p."apCombate", p."valCombate", p."apCombate2", p."valCombate2",
        p.add1, p."valAdd1", p.add2, p."valAdd2", p.add3, p."valAdd3", p.add4, p."valAdd4",
        p.inventario, p.dominios, p."kenActual", p."kiActual", p.positiva, p.negativa, p."vidaActual",
        p.hechizos, p.consumision, p.iniciativa, p.historia, p."tecEspecial", p.conviccion, p.cicatriz,
        p.notasaga, p.resistencia, p."pjPnj",p.marca, p.imagenurl, p.imagencloudid,p."imagenSeleccionada",p."coleccionImagenes",p.notas, p."usuarioId",
        
    a.spriteurl,
    a.filas,
    a.columnas,
    a.fps,
    a.scalesize,
    a.public_id 

  FROM personajes p
  LEFT JOIN animaciones a
    ON a.idpersonaje = p.idpersonaje

  WHERE p."usuarioId" = $1
  ORDER BY p.idpersonaje ASC
    `;
    const userResult = await pool.query(userQuery,[usuarioId]);

   
   if (userResult.rows.length === 0) {
  return res.status(200).json({
    message: 'Usuario sin personajes aún',
    coleccionPersonajes: [],  // ← importante
  });
}

    const coleccionPersonajes = userResult.rows;

   

    
    res.json({
      message: 'Peticion de personajes consumidos exitoso',
      coleccionPersonajes: coleccionPersonajes,   
    });

  } catch (error) {
    console.error('Error al obtener coleccion personajes del Usuario:', error);
    res.status(500).json({ message: 'Error en el servidor' });
  }
});

//Insert personaje OK!

app.post('/insert-personajeWPF', async (req, res) => {
  const {
    nombre, dominio, raza, naturaleza, edad, ken, ki, destino, pDestino,
    fuerza, fortaleza, destreza, agilidad, sabiduria, presencia, principio,
    sentidos, academisismo, alerta, atletismo, conBakemono, mentir, pilotear,
    artesMarciales, medicina, conObjMagicos, sigilo, conEsferas, conLeyendas,
    forja, conDemonio, conEspiritual, manejoBlaster, manejoSombras, tratoBakemono,
    conHechiceria, medVital, medEspiritual, rayo, fuego, frio, veneno, corte,
    energia, ventajas, apCombate, valCombate, apCombate2, valCombate2,
    add1, valAdd1, add2, valAdd2, add3, valAdd3, add4, valAdd4,
    imagen, inventario, dominios, kenActual, kiActual, positiva, negativa, vidaActual,
    hechizos, consumision, iniciativa, historia, usuarioId,
    tecEspecial, conviccion, cicatriz, notasaga, resistencia, pjPnj, notas, marca
  } = req.body;

  console.log(`
===== INSERT PERSONAJE =====
Usuario: ${usuarioId}
Nombre: ${nombre}
`);

  try {
    // 1. Insertar personaje sin imagenurl
    const query = `
      INSERT INTO personajes (
        nombre, dominio, raza, naturaleza, edad, ken, ki, destino, "pDestino",
        fuerza, fortaleza, destreza, agilidad, sabiduria, presencia, principio,
        sentidos, academisismo, alerta, atletismo, "conBakemono", mentir, pilotear,
        "artesMarciales", medicina, "conObjMagicos", sigilo, "conEsferas", "conLeyendas",
        forja, "conDemonio", "conEspiritual", "manejoBlaster", "manejoSombras", "tratoBakemono",
        "conHechiceria", "medVital", "medEspiritual", rayo, fuego, frio, veneno, corte,
        energia, ventajas, "apCombate", "valCombate", "apCombate2", "valCombate2",
        add1, "valAdd1", add2, "valAdd2", add3, "valAdd3", add4, "valAdd4",
        inventario, dominios, "kenActual", "kiActual", positiva, negativa, "vidaActual",
        hechizos, consumision, iniciativa, historia, "tecEspecial", conviccion, cicatriz,
        notasaga, resistencia, "pjPnj", notas, marca, "usuarioId"
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        $10, $11, $12, $13, $14, $15, $16,
        $17, $18, $19, $20, $21, $22, $23,
        $24, $25, $26, $27, $28, $29,
        $30, $31, $32, $33, $34, $35,
        $36, $37, $38, $39, $40, $41, $42, $43,
        $44, $45, $46, $47, $48, $49,
        $50, $51, $52, $53, $54, $55,
        $56, $57, $58, $59, $60, $61,
        $62, $63, $64, $65, $66, $67,
        $68, $69, $70, $71, $72, $73,
        $74, $75, $76, $77
      )
      RETURNING idpersonaje
    `;

    const values = [
      nombre, dominio, raza, naturaleza, edad, ken, ki, destino, pDestino,
      fuerza, fortaleza, destreza, agilidad, sabiduria, presencia, principio,
      sentidos, academisismo, alerta, atletismo, conBakemono, mentir, pilotear,
      artesMarciales, medicina, conObjMagicos, sigilo, conEsferas, conLeyendas,
      forja, conDemonio, conEspiritual, manejoBlaster, manejoSombras, tratoBakemono,
      conHechiceria, medVital, medEspiritual, rayo, fuego, frio, veneno, corte,
      energia, ventajas, apCombate, valCombate, apCombate2, valCombate2,
      add1, valAdd1, add2, valAdd2, add3, valAdd3, add4, valAdd4,
      inventario, dominios, kenActual, kiActual, positiva, negativa, vidaActual,
      hechizos, consumision, iniciativa, historia, tecEspecial, conviccion, cicatriz,
      notasaga, resistencia, pjPnj, notas, marca, usuarioId
    ];

    const result = await pool.query(query, values);
    const newId = result.rows[0].idpersonaje;

    let imageUrl = null;
    let imagenfileid = null;

    // 2. Subir la imagen a ImageKit
    if (imagen) {
      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({ error: 'Imagen base64 inválida.' });
      }

      const ext = matches[1];
      const data = matches[2];

      const uploadResult = await imagekit.files.upload({
        file: `data:image/${ext};base64,${data}`,
        fileName: `personaje_${newId}.${ext}`,
        folder: '/personajes',
        useUniqueFileName: false
      });

      imageUrl = uploadResult.url;
      imagenfileid = uploadResult.fileId;

      // 3. Actualizar URL e ID de ImageKit
      await pool.query(
        `UPDATE personajes
         SET imagenurl = $1, imagenfileid = $2
         WHERE idpersonaje = $3`,
        [imageUrl, imagenfileid, newId]
      );
    }

    // 4. Responder con éxito
    res.status(201).json({
      message: 'Personaje creado exitosamente.',
      idpersonaje: newId,
      imagenurl: imageUrl,
      imagenfileid: imagenfileid
    });

  } catch (err) {
    console.error('Error al insertar el personaje:', err);
    res.status(500).json({ error: 'Error al insertar el personaje.' });
  }
});



//Update personaje OK!
// Update personaje OK!
app.put('/update-personajeWPF/:id', async (req, res) => {
    const idpersonaje = req.params.id;

    let {
        nombre, dominio, raza, naturaleza, edad, ken, ki, destino, pDestino,
        fuerza, fortaleza, destreza, agilidad, sabiduria, presencia, principio,
        sentidos, academisismo, alerta, atletismo, conBakemono, mentir, pilotear,
        artesMarciales, medicina, conObjMagicos, sigilo, conEsferas, conLeyendas,
        forja, conDemonio, conEspiritual, manejoBlaster, manejoSombras, tratoBakemono,
        conHechiceria, medVital, medEspiritual, rayo, fuego, frio, veneno, corte,
        energia, ventajas, apCombate, valCombate, apCombate2, valCombate2,
        add1, valAdd1, add2, valAdd2, add3, valAdd3, add4, valAdd4,
        imagen, imagenurl, inventario, dominios, kenActual, kiActual,
        positiva, negativa, vidaActual, hechizos, consumision, iniciativa,
        historia, usuarioId, tecEspecial, conviccion, cicatriz, resistencia,
        imagenSeleccionada, pjPnj, notas, marca
    } = req.body;

    resistencia = resistencia === null || resistencia === undefined || resistencia === ""
        ? 0
        : resistencia;

    console.log("=================================");
    console.log("ACTUALIZANDO PERSONAJE");
    console.log("ID personaje:", idpersonaje);
    console.log("Nombre:", nombre);
    console.log("=================================");

    console.log("Imagen seleccionada en el backend:", imagenSeleccionada);

    try {

        let imagencloudid = null;

        // SUBIR IMAGEN A IMAGEKIT
        if (imagen && imagen.startsWith('data:image/')) {

            const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

            if (!matches) {
                return res.status(400).json({
                    error: 'Imagen base64 inválida.'
                });
            }

            const ext = matches[1];

            const nombreArchivo = `personaje_${idpersonaje}.${ext}`;

            const resultadoImagen = await subirImagenImageKit(
                imagen,
                'personajes',
                nombreArchivo
            );

            imagenurl = resultadoImagen.imagenurl;
            imagencloudid = resultadoImagen.imagencloudid;
        }

        // UPDATE COMPLETO
        const query = `
            UPDATE personajes SET
                nombre=$1, dominio=$2, raza=$3, naturaleza=$4, edad=$5,
                ken=$6, ki=$7, destino=$8, "pDestino"=$9, fuerza=$10,
                fortaleza=$11, destreza=$12, agilidad=$13, sabiduria=$14,
                presencia=$15, principio=$16, sentidos=$17, academisismo=$18,
                alerta=$19, atletismo=$20, "conBakemono"=$21, mentir=$22, pilotear=$23,
                "artesMarciales"=$24, medicina=$25, "conObjMagicos"=$26, sigilo=$27,
                "conEsferas"=$28, "conLeyendas"=$29, forja=$30, "conDemonio"=$31,
                "conEspiritual"=$32, "manejoBlaster"=$33, "manejoSombras"=$34,
                "tratoBakemono"=$35, "conHechiceria"=$36, "medVital"=$37, "medEspiritual"=$38,
                rayo=$39, fuego=$40, frio=$41, veneno=$42, corte=$43, energia=$44, ventajas=$45,
                "apCombate"=$46, "valCombate"=$47, "apCombate2"=$48, "valCombate2"=$49,
                add1=$50, "valAdd1"=$51, add2=$52, "valAdd2"=$53, add3=$54, "valAdd3"=$55,
                add4=$56, "valAdd4"=$57, inventario=$58, dominios=$59, "kenActual"=$60,
                "kiActual"=$61, positiva=$62, negativa=$63, "vidaActual"=$64,
                hechizos=$65, consumision=$66, iniciativa=$67, historia=$68,
                "usuarioId"=$69, "tecEspecial"=$70, conviccion=$71, cicatriz=$72,
                resistencia=$73, "pjPnj"=$74, imagenurl=$75,
                "imagenSeleccionada"=$76, notas=$77, marca=$78
            WHERE idpersonaje=$79
        `;

        const values = [
            nombre, dominio, raza, naturaleza, edad, ken, ki, destino, pDestino,
            fuerza, fortaleza, destreza, agilidad, sabiduria, presencia, principio,
            sentidos, academisismo, alerta, atletismo, conBakemono, mentir, pilotear,
            artesMarciales, medicina, conObjMagicos, sigilo, conEsferas, conLeyendas,
            forja, conDemonio, conEspiritual, manejoBlaster, manejoSombras, tratoBakemono,
            conHechiceria, medVital, medEspiritual, rayo, fuego, frio, veneno, corte,
            energia, ventajas, apCombate, valCombate, apCombate2, valCombate2,
            add1, valAdd1, add2, valAdd2, add3, valAdd3, add4, valAdd4,
            inventario, dominios, kenActual, kiActual, positiva, negativa, vidaActual,
            hechizos, consumision, iniciativa, historia, usuarioId,
            tecEspecial, conviccion, cicatriz, resistencia, pjPnj,
            imagenurl, imagenSeleccionada, notas, marca, idpersonaje
        ];

        // DIAGNÓSTICO DE CAMPOS VACÍOS
        const nombresCampos = [
            "nombre", "dominio", "raza", "naturaleza", "edad",
            "ken", "ki", "destino", "pDestino", "fuerza",
            "fortaleza", "destreza", "agilidad", "sabiduria",
            "presencia", "principio", "sentidos", "academisismo",
            "alerta", "atletismo", "conBakemono", "mentir",
            "pilotear", "artesMarciales", "medicina",
            "conObjMagicos", "sigilo", "conEsferas",
            "conLeyendas", "forja", "conDemonio",
            "conEspiritual", "manejoBlaster",
            "manejoSombras", "tratoBakemono",
            "conHechiceria", "medVital", "medEspiritual",
            "rayo", "fuego", "frio", "veneno", "corte",
            "energia", "ventajas", "apCombate",
            "valCombate", "apCombate2", "valCombate2",
            "add1", "valAdd1", "add2", "valAdd2",
            "add3", "valAdd3", "add4", "valAdd4",
            "inventario", "dominios", "kenActual",
            "kiActual", "positiva", "negativa",
            "vidaActual", "hechizos", "consumision",
            "iniciativa", "historia", "usuarioId",
            "tecEspecial", "conviccion", "cicatriz",
            "resistencia", "pjPnj", "imagenurl",
            "imagenSeleccionada", "notas", "marca", "idpersonaje"
        ];

        values.forEach((valor, index) => {
            if (valor === "") {
                console.log("=================================");
                console.log("CAMPO VACÍO DETECTADO:");
                console.log("Campo:", nombresCampos[index]);
                console.log("Posición SQL:", index + 1);
                console.log("=================================");
            }
        });

        await pool.query(query, values);

        res.status(201).json({
            message: 'Personaje modificado exitosamente.',
            idpersonaje,
            imagenurl,
            imagencloudid
        });

    } catch (err) {

        console.error("=================================");
        console.error("ERROR AL ACTUALIZAR PERSONAJE");
        console.error("ID personaje:", idpersonaje);
        console.error("Mensaje:", err.message);
        console.error("Código PostgreSQL:", err.code);
        console.error("Detalle:", err.detail);
        console.error("Dónde:", err.where);
        console.error("Tabla:", err.table);
        console.error("Columna:", err.column);
        console.error("Restricción:", err.constraint);
        console.error("Stack completo:", err.stack);
        console.error("=================================");

        res.status(500).json({
            error: err.message,
            code: err.code,
            detail: err.detail
        });
    }
});

//OK!!
app.get('/consumirPersonajesRankingWPF', async (req, res) => {
    try {
        const userQuery = `
            SELECT 
                p.idpersonaje, p.nombre, p.dominio, p.raza, p.naturaleza,
                p.edad, p.ken, p.ki, p.destino, p."pDestino",
                p.fuerza, p.fortaleza, p.destreza, p.agilidad,
                p.sabiduria, p.presencia, p.principio,
                p.sentidos, p.academisismo, p.alerta, p.atletismo,
                p."conBakemono", p.mentir, p.pilotear,
                p."artesMarciales", p.medicina, p."conObjMagicos",
                p.sigilo, p."conEsferas", p."conLeyendas",
                p.forja, p."conDemonio", p."conEspiritual",
                p."manejoBlaster", p."manejoSombras", p."tratoBakemono",
                p."conHechiceria", p."medVital", p."medEspiritual",
                p.rayo, p.fuego, p.frio, p.veneno, p.corte,
                p.energia, p.ventajas, p."apCombate", p."valCombate",
                p."apCombate2", p."valCombate2",
                p.add1, p."valAdd1", p.add2, p."valAdd2",
                p.add3, p."valAdd3", p.add4, p."valAdd4",
                p.inventario, p.dominios, p."kenActual", p."kiActual",
                p.positiva, p.negativa, p."vidaActual",
                p.hechizos, p.consumision, p.iniciativa,
                p.historia, p."tecEspecial", p.conviccion, p.cicatriz,
                p.notasaga, p.resistencia, p."pjPnj",
                p.imagenurl, p.imagencloudid, p."imagenSeleccionada",
                p."coleccionImagenes", p."usuarioId", p.notas,
                a.spriteurl, a.filas, a.columnas, a.fps,
                a.scalesize, a.public_id
            FROM personajes p
            LEFT JOIN animaciones a
                ON a.idpersonaje = p.idpersonaje
            WHERE p.ken >= 100
              AND p.marca = TRUE
        `;

        const userResult = await pool.query(userQuery);

        if (userResult.rows.length === 0) {
            return res.status(404).json({
                message: 'No se encontraron personajes con Ken igual o superior a 100 y marca activa'
            });
        }

        res.json({
            message: 'Se consumieron los personajes con marca activa',
            coleccionPersonajes: userResult.rows
        });

    } catch (error) {
        console.error(
            'Error al obtener personajes marcados de la base de datos:',
            error
        );

        res.status(500).json({
            message: 'Error en el servidor'
        });
    }
});

//*************************************************
// SAGAS
// INSERT SAGA OK ✅

// INSERT SAGA
app.post('/insert-saga', async (req, res) => {
    const {
        titulo,
        presentacion,
        imagensaga,
        personajes
    } = req.body;

    try {

        // 1. Insertar saga base
        const insertQuery = `
            INSERT INTO sagas (titulo, presentacion, imagensaga, personajes)
            VALUES ($1, $2, $3, $4)
            RETURNING idsaga
        `;

        const insertValues = [titulo, presentacion, imagensaga, personajes];

        const insertResult = await pool.query(insertQuery, insertValues);

        const newSagaId = insertResult.rows[0].idsaga;

        // Imagen por defecto
        let imagenUrl = "https://res.cloudinary.com/dzul1hatw/image/upload/v1774459888/imagenBase_whvcot.jpg";
        let imagenCloudId = null;

        // 2. Subir imagen a ImageKit
        if (
            imagensaga &&
            typeof imagensaga === 'string' &&
            imagensaga.startsWith('data:image/')
        ) {

            const matches = imagensaga.match(/^data:image\/(\w+);base64,(.+)$/);

            if (matches) {

                const ext = matches[1];

                try {

                    const resultadoImagen = await subirImagenImageKit(
                        imagensaga,
                        'sagas',
                        `saga_${newSagaId}.${ext}`
                    );

                    imagenUrl = resultadoImagen.imagenurl;
                    imagenCloudId = resultadoImagen.imagencloudid;

                } catch (uploadError) {

                    console.warn(
                        "Error subiendo a ImageKit, se usa imagen default:",
                        uploadError.message
                    );
                }

            } else {
                console.warn("Imagen base64 inválida, se usa imagen default");
            }
        }

        // 3. Guardar imagen
        await pool.query(
            `UPDATE sagas
             SET imagenurl = $1, imagencloudid = $2
             WHERE idsaga = $3`,
            [imagenUrl, imagenCloudId, newSagaId]
        );

        // 4. Respuesta
        res.status(201).json({
            message: 'Saga creada exitosamente.',
            idsaga: newSagaId,
            imagenurl: imagenUrl,
            imagencloudid: imagenCloudId
        });

    } catch (error) {

        console.error('Error al insertar la saga:', error);

        res.status(500).json({
            error: 'Error al insertar la saga.'
        });
    }
});

//consmumir sagas ok!!
// CONSUMIR SAGAS OK
app.get('/consumirSagas', async (req, res) => {
    try {

        const result = await pool.query(`
            SELECT
                idsaga,
                titulo,
                presentacion,
                personajes,
                imagenurl,
                imagencloudid
            FROM sagas
            ORDER BY idsaga DESC
        `);

        res.status(200).json({
            coleccionSagas: result.rows
        });

    } catch (error) {

        console.error('Error al obtener sagas:', error);

        res.status(500).json({
            error: 'Error interno al obtener sagas'
        });
    }
});

//update sagas y secciones ok!!

// UPDATE SAGAS Y SECCIONES
app.put('/updateSagaCompleta/:idsaga', async (req, res) => {

    const { idsaga } = req.params;

    const {
        titulo,
        presentacion,
        imagensaga,
        secciones
    } = req.body;

    if (!presentacion || !titulo) {
        return res.status(400).json({
            error: 'Título y presentación son requeridos'
        });
    }

    try {

        let imagenurl = null;
        let imagencloudid = null;

        // 1. ACTUALIZAR IMAGEN DE LA SAGA
        if (
            imagensaga &&
            typeof imagensaga === 'string' &&
            imagensaga.startsWith('data:image/')
        ) {

            const matches = imagensaga.match(/^data:image\/(\w+);base64,(.+)$/);

            if (!matches) {
                return res.status(400).json({
                    error: 'Formato de imagen base64 inválido para saga'
                });
            }

            const ext = matches[1];

            const resultadoImagen = await subirImagenImageKit(
                imagensaga,
                'sagas',
                `saga_${idsaga}.${ext}`
            );

            imagenurl = resultadoImagen.imagenurl;
            imagencloudid = resultadoImagen.imagencloudid;

            await pool.query(
                `UPDATE sagas
                 SET imagenurl = $1, imagencloudid = $2
                 WHERE idsaga = $3`,
                [imagenurl, imagencloudid, idsaga]
            );
        }

        // 2. ACTUALIZAR DATOS DE LA SAGA
        const resultSaga = await pool.query(
            `UPDATE sagas
             SET titulo = $1, presentacion = $2
             WHERE idsaga = $3
             RETURNING *`,
            [titulo, presentacion, idsaga]
        );

        if (resultSaga.rowCount === 0) {
            return res.status(404).json({
                error: 'Saga no encontrada'
            });
        }

        // 3. ARRAY PARA ACUMULAR SECCIONES
        const seccionesActualizadas = [];

        // 4. ACTUALIZAR O INSERTAR SECCIONES
        if (Array.isArray(secciones)) {

            for (const seccion of secciones) {

                const {
                    idseccion,
                    titulo,
                    presentacion,
                    imagen
                } = seccion;

                let idsec = idseccion;

                let imagenurlSeccion = null;
                let imagenfileidSeccion = null;

                // SUBIR IMAGEN DE SECCIÓN A IMAGEKIT
                if (
                    imagen &&
                    typeof imagen === 'string' &&
                    imagen.startsWith('data:image/')
                ) {

                    const matchesSeccion = imagen.match(
                        /^data:image\/(\w+);base64,(.+)$/
                    );

                    if (!matchesSeccion) {
                        return res.status(400).json({
                            error: `Formato de imagen base64 inválido para sección ${idseccion || 'nueva'}`
                        });
                    }

                    const ext = matchesSeccion[1];

                    const nombreArchivo = idseccion
                        ? `seccion_${idseccion}.${ext}`
                        : `seccion_saga_${idsaga}_${Date.now()}.${ext}`;

                    const resultadoImagenSeccion = await subirImagenImageKit(
                        imagen,
                        'secciones',
                        nombreArchivo
                    );

                    imagenurlSeccion = resultadoImagenSeccion.imagenurl;
                    imagenfileidSeccion = resultadoImagenSeccion.imagencloudid;
                }

                // 5. NUEVA SECCIÓN
                if (!idsec) {

                    const insertResult = await pool.query(
                        `INSERT INTO secciones (
                            idsaga,
                            titulo,
                            presentacion,
                            imagenurl,
                            imagenfileid
                        )
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING idseccion`,
                        [
                            idsaga,
                            titulo,
                            presentacion,
                            imagenurlSeccion,
                            imagenfileidSeccion
                        ]
                    );

                    idsec = insertResult.rows[0].idseccion;

                } else {

                    // 6. ACTUALIZAR SECCIÓN EXISTENTE
                    await pool.query(
                        `UPDATE secciones
                         SET titulo = $1,
                             presentacion = $2,
                             imagenurl = COALESCE($3, imagenurl),
                             imagenfileid = COALESCE($4, imagenfileid)
                         WHERE idseccion = $5`,
                        [
                            titulo,
                            presentacion,
                            imagenurlSeccion,
                            imagenfileidSeccion,
                            idsec
                        ]
                    );
                }

                // 7. GUARDAR SECCIÓN ACTUALIZADA
                seccionesActualizadas.push({
                    idseccion: idsec,
                    titulo,
                    presentacion,
                    imagenurl: imagenurlSeccion,
                    imagenfileid: imagenfileidSeccion
                });
            }
        }

        // 8. RESPUESTA
        res.json({
            message: 'Saga y secciones actualizadas correctamente',
            saga: resultSaga.rows[0],
            imagenurl,
            imagencloudid,
            secciones: seccionesActualizadas
        });

    } catch (error) {

        console.error('Error al actualizar saga y secciones:', error);

        res.status(500).json({
            error: 'Error interno al actualizar saga y secciones'
        });
    }
});

// Endpoint para agregar personaje a una saga
app.put('/agregarPersonajeSaga/:idsaga', async (req, res) => {
  const { idsaga } = req.params;
  const { personajes } = req.body;

  if (!personajes || !Array.isArray(personajes)) {
    return res.status(400).json({ error: 'El campo personajes es requerido y debe ser un array' });
  }

  try {
    const result = await pool.query(
      `UPDATE sagas SET personajes = $1 WHERE idsaga = $2 RETURNING *`,
      [personajes, idsaga]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Saga no encontrada' });
    }

    res.json({ message: 'Personaje agregado correctamente', sagaActualizada: result.rows[0] });
  } catch (error) {
    console.error('Error al actualizar saga:', error.message);
    res.status(500).json({ error: 'Error del servidor al actualizar la saga' });
  }
});

app.put("/eliminarPersonajeSaga/:idsaga", async (req, res) => {
  const { idsaga } = req.params;
  const { personajes } = req.body;

  //console.log("🟡 [DELETE PERSONAJE] idsaga:", idsaga);
  //console.log("🟡 [DELETE PERSONAJE] personajes recibidos:", personajes);

  if (!personajes || !Array.isArray(personajes)) {
    return res
      .status(400)
      .json({ message: "El campo 'personajes' debe ser un array válido" });
  }

  try {
    // Filtramos nulls, undefined o vacíos
    const personajesFiltrados = personajes.filter((id) => Number.isInteger(id));

    // ✅ Convertimos el array JS a un literal válido para PostgreSQL: {1,2,3}
    const personajesArrayLiteral = `{${personajesFiltrados.join(",")}}`;

    // ✅ Ejecutamos la query
    const result = await pool.query(
      "UPDATE sagas SET personajes = $1 WHERE idsaga = $2",
      [personajesArrayLiteral, idsaga]
    );

    console.log("🟢 Filas afectadas:", result.rowCount);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "Saga no encontrada" });
    }

    res.status(200).json({
      message: "Personaje eliminado de la saga correctamente",
      personajesActualizados: personajesFiltrados,
    });
  } catch (error) {
    console.error("🔥 Error al eliminar personaje de saga:", error);
    res.status(500).json({
      message: "Error en el servidor",
      error: error.message,
    });
  }
});



//PARA LA SECCION DE LA SAGA  ok!!
app.get('/consumirSecciones', async (req, res) => {
  const { idsaga } = req.query;

  try {
    const result = await pool.query(
      'SELECT * FROM secciones WHERE idsaga = $1',
      [idsaga]
    );
    res.status(200).json({ coleccionSecciones: result.rows });
  } catch (error) {
    console.error('Error al obtener secciones:', error);
    res.status(500).json({ error: 'Error interno al obtener secciones' });
  }
});




//notas 
app.put('/personajes/:idpersonaje/notasaga', async (req, res) => {
  const { idpersonaje } = req.params;
  const { notasaga } = req.body;

  //console.log(" id personaje ",idpersonaje)
 // console.log("notasaga  ", notasaga)

  if (!notasaga) {
    return res.status(400).json({ error: 'Faltan datos de notasaga' });
  }

  try {
    const result = await pool.query(
      `UPDATE personajes SET notasaga = $1 WHERE idpersonaje = $2`,
      [JSON.stringify(notasaga), idpersonaje]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Personaje no encontrado' });
    }

    res.status(200).json({ message: 'Notasaga actualizada correctamente' });
  } catch (error) {
    console.error('Error al actualizar notasaga:', error);
    res.status(500).json({ error: 'Error en el servidor' });
  }
});


// UPDATE USUARIOS
app.put('/updateUsuarios/:usuarioId', async (req, res) => {

    const usuarioId = req.params.usuarioId;
    const { nick, email, contrasenia, imagenurl } = req.body;

    if (!usuarioId) {
        return res.status(400).json({
            error: 'ID de usuario es obligatorio'
        });
    }

    if (
        typeof nick !== 'string' ||
        typeof email !== 'string' ||
        typeof contrasenia !== 'string'
    ) {
        return res.status(400).json({
            error: 'Datos inválidos'
        });
    }

    let imagenUrlFinal = null;
    let imagenFileIdFinal = null;

    try {

        // 1. SUBIR IMAGEN A IMAGEKIT
        if (
            imagenurl &&
            typeof imagenurl === 'string' &&
            imagenurl.startsWith('data:image/')
        ) {

            const matches = imagenurl.match(/^data:image\/(\w+);base64,(.+)$/);

            if (!matches) {
                return res.status(400).json({
                    error: 'Imagen base64 inválida.'
                });
            }

            const ext = matches[1];

            const resultadoImagen = await subirImagenImageKit(
                imagenurl,
                'usuarios',
                `usuario_${usuarioId}.${ext}`
            );

            imagenUrlFinal = resultadoImagen.imagenurl;
            imagenFileIdFinal = resultadoImagen.imagencloudid;
        }

        // 2. ARMAR CONSULTA DE ACTUALIZACIÓN
        let updateQuery = `
            UPDATE usuarios
            SET nick = $1,
                email = $2,
                contrasenia = $3
        `;

        const values = [nick, email, contrasenia];

        let paramIndex = 4;

        if (imagenUrlFinal && imagenFileIdFinal) {

            updateQuery += `,
                imagenurl = $${paramIndex},
                imagenfileid = $${paramIndex + 1}
            `;

            values.push(imagenUrlFinal, imagenFileIdFinal);

            paramIndex += 2;
        }

        updateQuery += `
            WHERE idusuario = $${paramIndex}
            RETURNING *
        `;

        values.push(usuarioId);

        // 3. EJECUTAR UPDATE
        const result = await pool.query(updateQuery, values);

        if (result.rowCount === 0) {
            return res.status(404).json({
                error: 'Usuario no encontrado'
            });
        }

        // 4. DEVOLVER DATOS ACTUALIZADOS
        const usuarioActualizado = result.rows[0];

        res.status(200).json({
            mensaje: 'Usuario actualizado',
            nick: usuarioActualizado.nick,
            email: usuarioActualizado.email,
            contrasenia: usuarioActualizado.contrasenia,
            imagenurl: usuarioActualizado.imagenurl || "",
            imagenfileid: usuarioActualizado.imagenfileid || ""
        });

    } catch (error) {

        console.error('Error actualizando usuario:', error);

        res.status(500).json({
            error: 'Error del servidor'
        });
    }
});





//*************************Sprite ***********************************

// ******************** SPRITE ********************

app.post("/upload-sprite", async (req, res) => {

    const { idpersonaje, imagen, filas, columnas, fps, scalesize } = req.body;

    try {

        if (!idpersonaje) {
            return res.status(400).json({
                error: "idpersonaje requerido"
            });
        }

        const f = Number(filas);
        const c = Number(columnas);
        const velocidad = Number(fps);
        const scale = Number(scalesize) || 0.8;

        if ([f, c, velocidad].some(v => Number.isNaN(v))) {
            return res.status(400).json({
                error: "Datos de animación inválidos"
            });
        }

        let imageUrl = null;
        let fileId = null;

        // SOLO SI VIENE IMAGEN NUEVA
        if (imagen) {

            const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

            if (!matches) {
                return res.status(400).json({
                    error: "Imagen base64 inválida"
                });
            }

            const ext = matches[1];

            // SUBIR A IMAGEKIT
            const resultadoImagen = await subirImagenImageKit(
                imagen,
                'sprites',
                `sprite_${idpersonaje}.${ext}`
            );

            imageUrl = resultadoImagen.imagenurl;
            fileId = resultadoImagen.imagencloudid;
        }

        // UPSERT
        const query = `
            INSERT INTO animaciones (
                idpersonaje,
                spriteurl,
                filas,
                columnas,
                fps,
                scalesize,
                public_id
            )
            VALUES (
                $1,
                COALESCE($2, (SELECT spriteurl FROM animaciones WHERE idpersonaje=$1)),
                $3,
                $4,
                $5,
                $6,
                COALESCE($7, (SELECT public_id FROM animaciones WHERE idpersonaje=$1))
            )
            ON CONFLICT (idpersonaje)
            DO UPDATE SET
                spriteurl = COALESCE(EXCLUDED.spriteurl, animaciones.spriteurl),
                filas = EXCLUDED.filas,
                columnas = EXCLUDED.columnas,
                fps = EXCLUDED.fps,
                scalesize = EXCLUDED.scalesize,
                public_id = COALESCE(EXCLUDED.public_id, animaciones.public_id)
            RETURNING *;
        `;

        const result = await pool.query(query, [
            idpersonaje,
            imageUrl,
            f,
            c,
            velocidad,
            scale,
            fileId
        ]);

        return res.status(200).json({
            message: "Sprite actualizado correctamente",
            animacion: result.rows[0]
        });

    } catch (err) {

        console.error("Error upload sprite:", err);

        return res.status(500).json({
            error: "Error al subir sprite"
        });
    }
});



app.put("/eliminarSprite", async (req, res) => {
  const { idpersonaje } = req.body;

  try {
    if (!idpersonaje) {
      return res.status(400).json({
        error: "Falta idpersonaje",
      });
    }

    // 1. Recuperar identificador de ImageKit
    const spriteResult = await pool.query(
      `SELECT public_id FROM animaciones WHERE idpersonaje = $1`,
      [idpersonaje]
    );

    if (spriteResult.rows.length === 0) {
      return res.status(404).json({
        error: "Animación no encontrada",
      });
    }

    const fileId = spriteResult.rows[0].public_id;

    // 2. Eliminar imagen de ImageKit
    if (fileId) {
      await imagekit.deleteFile(fileId);
      console.log("ImageKit deleteFile:", fileId);
    }

    // 3. Limpiar animación en PostgreSQL
    const result = await pool.query(
      `
      UPDATE animaciones
      SET
        spriteurl = NULL,
        public_id = NULL,
        filas = 3,
        columnas = 4,
        fps = 6
      WHERE idpersonaje = $1
      RETURNING spriteurl, filas, columnas, fps, public_id
      `,
      [idpersonaje]
    );

    res.json({
      ok: true,
      animacion: result.rows[0],
    });

  } catch (err) {
    console.log("ERROR eliminarSprite:", err);

    res.status(500).json({
      error: "No se pudo eliminar el sprite",
    });
  }
});

//*********************************************** 








//consmumir objetos ok!!
app.get('/consumirObjetosMagicos', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT *
      FROM tesoros
    `);

    res.status(200).json({ objetosMagicos: result.rows });
  } catch (error) {
    console.error('Error al consumir objetos magicos:', error);
    res.status(500).json({ error: 'Error interno al obtener objetos magicos' });
  }
});

//OK

app.post('/insertObjetoMagico', async (req, res) => {
  const {
    nombre, rareza, nivel, costeVentaja, precio, descripcion, sistema, imagen
  } = req.body;

  try {
    // 1. Insertar tesoro sin imagen
    const query = `
      INSERT INTO tesoros (
        nombre, rareza, nivel, "costeVentaja", precio, descripcion, sistema
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING idobjeto
    `;

    const values = [
      nombre, rareza, nivel, costeVentaja, precio, descripcion, sistema
    ];

    const result = await pool.query(query, values);
    const newId = result.rows[0].idobjeto;

    let imageUrl = null;

    if (imagen) {
      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({
          error: 'Imagen base64 inválida.'
        });
      }

      const ext = matches[1];

      // 2. Subir imagen a ImageKit
      const uploadResult = await imagekit.upload({
        file: imagen,
        fileName: `tesoro_${newId}.${ext}`,
        folder: '/tesoros',
        useUniqueFileName: false,
      });

      imageUrl = uploadResult.url;
      const imageFileId = uploadResult.fileId;

      // 3. Actualizar URL e ID de ImageKit
      await pool.query(
        'UPDATE tesoros SET imagenurl = $1, imagenfileid = $2 WHERE idobjeto = $3',
        [imageUrl, imageFileId, newId]
      );
    }

    // 4. Responder
    res.status(201).json({
      message: 'tesoro creado exitosamente.',
      idobjeto: newId,
      imagenurl: imageUrl,
    });

  } catch (err) {
    console.error('Error al insertar objeto magico nuevo:', err);

    res.status(500).json({
      error: 'Error al insertar objeto magico nuevo.'
    });
  }
});


//ok!!

app.put('/updateObjetoMagico/:id', async (req, res) => {
  const { id } = req.params;
  const {
    nombre,
    rareza,
    nivel,
    costeVentaja,
    precio,
    descripcion,
    sistema,
    imagen,
  } = req.body;

  try {
    let imageUrl = null;
    let imageFileId = null;

    // 1. Obtener ID de la imagen anterior
    const anteriorResult = await pool.query(
      'SELECT imagenfileid FROM tesoros WHERE idobjeto = $1',
      [id]
    );

    if (anteriorResult.rowCount === 0) {
      return res.status(404).json({ error: 'Objeto mágico no encontrado.' });
    }

    const imagenAnterior = anteriorResult.rows[0].imagenfileid;

    // 2. Si viene una nueva imagen en base64, subirla a ImageKit
    if (imagen && imagen.startsWith('data:image/')) {
      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({ error: 'Imagen base64 inválida.' });
      }

      const ext = matches[1];

      const uploadResult = await imagekit.upload({
        file: imagen,
        fileName: `tesoro_${id}.${ext}`,
        folder: '/tesoros',
        useUniqueFileName: false,
      });

      imageUrl = uploadResult.url;
      imageFileId = uploadResult.fileId;
    }

    // 3. Armar la query de actualización
    let updateQuery = `
      UPDATE tesoros SET
        nombre = $1,
        rareza = $2,
        nivel = $3,
        "costeVentaja" = $4,
        precio = $5,
        descripcion = $6,
        sistema = $7
    `;

    const updateValues = [
      nombre,
      rareza,
      nivel,
      costeVentaja,
      precio,
      descripcion,
      sistema,
    ];

    if (imageUrl) {
      updateQuery += `, imagenurl = $8, imagenfileid = $9`;
      updateValues.push(imageUrl, imageFileId);
    }

    updateQuery += ` WHERE idobjeto = $${updateValues.length + 1}`;
    updateValues.push(id);

    await pool.query(updateQuery, updateValues);

    // 4. Eliminar imagen anterior de ImageKit
    if (imageFileId && imagenAnterior && imagenAnterior !== imageFileId) {
      await imagekit.deleteFile(imagenAnterior);
    }

    res.json({
      message: 'Objeto mágico actualizado exitosamente.'
    });

  } catch (err) {
    console.error('Error al actualizar objeto mágico:', err);
    res.status(500).json({
      error: 'Error al actualizar objeto mágico.'
    });
  }
});


//ok!!
app.delete('/deleteObjetoMagico/:idobjeto', async (req, res) => {
  const { idobjeto } = req.params;
    //console.log('Intentando eliminar objeto con id:', idobjeto);

  try {
    // Opcional: verificar que el objeto exista antes de eliminar

    const deleteQuery = 'DELETE FROM tesoros WHERE idobjeto = $1';
    const result = await pool.query(deleteQuery, [idobjeto]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Objeto mágico no encontrado.' });
    }

    res.json({ message: 'Objeto mágico eliminado correctamente.' });
  } catch (error) {
    console.error('Error al eliminar objeto mágico:', error);
    res.status(500).json({ error: 'Error al eliminar objeto mágico.' });
  }
});






//************************LOGROS APP MOBILE *****************************
app.get('/consumirLogros', async (req, res) => {
  try {
    
    //const { usuarioId } = req.query;
   // console.log("el id del usuario es: ",usuarioId)
    const userQuery = `
      SELECT *
      FROM logros;
    `;




    const userResult = await pool.query(userQuery);

   
   if (userResult.rows.length === 0) {
  return res.status(200).json({
    message: 'Usuario sin personajes aún',
    logrosConsumidos: [],  // ← importante
  });
}

    const logrosConsumidos = userResult.rows;


  
   

    
    res.json({
      message: 'Peticion de logros exitosa!',
      logrosConsumidos: logrosConsumidos,   
    });

  } catch (error) {
    console.error('Error al obtener logros consumidos de la base de datos:', error);
    res.status(500).json({ message: 'Error en el servidor' });
  }
});




app.post('/insertarLogro', async (req, res) => {
  try {
    const { nombre, descripcion, categoria, nivel, imagen, personajesids } = req.body;

    console.log("", personajesids);

    if (!nombre || nombre.trim() === '') {
      return res.status(400).json({ error: 'El campo nombre es obligatorio' });
    }

    // 1. Insertar el logro sin imagen
    const query = `
      INSERT INTO logros (nombre, descripcion, categoria, nivel, personajesids)
      VALUES ($1, $2, $3, $4, $5)
      RETURNING *;
    `;

    const values = [nombre, descripcion, categoria, nivel, personajesids];
    const result = await pool.query(query, values);
    const newId = result.rows[0].id;

    let imageUrl = null;

    // 2. Subir la imagen a ImageKit si existe
    if (imagen) {
      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({ error: 'Imagen base64 inválida.' });
      }

      const ext = matches[1];

      const uploadResult = await imagekit.upload({
        file: imagen,
        fileName: `logros_${newId}.${ext}`,
        folder: '/logros',
        useUniqueFileName: false,
      });

      imageUrl = uploadResult.url;
      const imageFileId = uploadResult.fileId;

      // 3. Actualizar URL e ID de ImageKit
      await pool.query(
        'UPDATE logros SET imagenurl = $1, imagenfileid = $2 WHERE id = $3',
        [imageUrl, imageFileId, newId]
      );
    }

    // 4. Recuperar el logro completo
    const updatedResult = await pool.query(
      'SELECT * FROM logros WHERE id = $1',
      [newId]
    );

    const logroCompleto = updatedResult.rows[0];

    // 5. Responder con el logro completo
    return res.status(201).json({
      message: 'Logro insertado correctamente',
      logro: logroCompleto,
    });

  } catch (error) {
    console.error('Error en /insertarLogro:', error);
    return res.status(500).json({
      error: 'Error del servidor al insertar logro'
    });
  }
});



app.delete('/eliminarLogro/:id', async (req, res) => {
  const { id } = req.params;

  try {
    // Verifica que el logro exista
    const { rowCount: existe } = await pool.query(
      'SELECT 1 FROM logros WHERE id = $1',
      [id]
    );

    if (!existe) {
      return res.status(404).json({ message: 'Logro no encontrado' });
    }

    // Elimina el logro
    await pool.query('DELETE FROM logros WHERE id = $1', [id]);

    return res.json({ message: 'Logro eliminado exitosamente' });
  } catch (error) {
    console.error('Error al eliminar logro:', error);
    return res.status(500).json({ message: 'Error interno del servidor' });
  }
});




app.put('/editarLogro/:id', async (req, res) => {
  const logroId = req.params.id;
  const { nombre, descripcion, categoria, nivel, imagen, personajesids } = req.body;

  if (!nombre || !descripcion) {
    return res.status(400).json({ error: 'Nombre y descripción son requeridos' });
  }

  try {
    // 1. Actualizar datos básicos
    const queryUpdate = `
      UPDATE logros
      SET nombre = $1,
          descripcion = $2,
          categoria = $3,
          nivel = $4,
          personajesids = $5
      WHERE id = $6
      RETURNING *;
    `;

    const values = [nombre, descripcion, categoria, nivel, personajesids, logroId];

    let result = await pool.query(queryUpdate, values);
    let logroActualizado = result.rows[0];

    // 2. Si viene una imagen en base64, subirla a ImageKit
    if (imagen && imagen.startsWith('data:image')) {
      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({ error: 'Imagen base64 inválida.' });
      }

      const ext = matches[1];

      const uploadResult = await imagekit.upload({
        file: imagen,
        fileName: `logros_${logroId}.${ext}`,
        folder: '/logros',
        useUniqueFileName: false,
      });

      const imageUrl = uploadResult.url;
      const imageFileId = uploadResult.fileId;

      // 3. Actualizar los campos de imagen
      const updateImageQuery = `
        UPDATE logros
        SET imagenurl = $1,
            imagenfileid = $2
        WHERE id = $3
        RETURNING *;
      `;

      result = await pool.query(updateImageQuery, [
        imageUrl,
        imageFileId,
        logroId
      ]);

      logroActualizado = result.rows[0];
    }

    // 4. Devolver logro actualizado
    res.json({
      message: 'Logro actualizado correctamente',
      logro: logroActualizado
    });

  } catch (err) {
    console.error('Error al editar logro:', err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});


//**************************************************************************** 





//****************************Dispositivos Neo App MOBILE************************************
app.get('/consumirObjetosNeo', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT *
      FROM dispositivos
    `);

    res.status(200).json({ objetosNeo: result.rows });
  } catch (error) {
    console.error('Error al consumir dispositivos Neotecnicos:', error);
    res.status(500).json({ error: 'Error interno al obtener dispositivos Neotecnicos' });
  }
});

//OK

app.post('/insertObjetoNeo', async (req, res) => {

  console.log(" red : ", req.body);

  const {
    nombre, rareza, nivel, costeVentaja, precio, descripcion, sistema, imagen
  } = req.body;

  try {
    // 1. Insertar dispositivo sin imagen
    const query = `
      INSERT INTO dispositivos (
        nombre, rareza, nivel, "costeVentaja", precio, descripcion, sistema
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7
      )
      RETURNING idobjeto
    `;

    const values = [
      nombre, rareza, nivel, costeVentaja, precio, descripcion, sistema
    ];

    const result = await pool.query(query, values);
    const newId = result.rows[0].idobjeto;

    let imageUrl = null;

    if (imagen) {
      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({ error: 'Imagen base64 inválida.' });
      }

      const ext = matches[1];

      // 2. Subir imagen a ImageKit
      const uploadResult = await imagekit.upload({
        file: imagen,
        fileName: `dispositivo_${newId}.${ext}`,
        folder: '/dispositivos',
        useUniqueFileName: false,
      });

      imageUrl = uploadResult.url;
      const imageFileId = uploadResult.fileId;

      // 3. Actualizar URL e ID de ImageKit en la base de datos
      await pool.query(
        'UPDATE dispositivos SET imagenurl = $1, imagenfileid = $2 WHERE idobjeto = $3',
        [imageUrl, imageFileId, newId]
      );
    }

    // 4. Responder con éxito
    res.status(201).json({
      message: 'dispositivo creado exitosamente.',
      idobjeto: newId,
      imagenurl: imageUrl,
    });

  } catch (err) {
    console.error('Error al insertar dispositivo Neotecnico nuevo:', err);
    res.status(500).json({
      error: 'Error al insertar dispositivo Neotecnico nuevo.'
    });
  }
});


//ok!!

app.put('/updateObjetoNeo/:id', async (req, res) => {
  const { id } = req.params;
  const {
    nombre,
    rareza,
    nivel,
    costeVentaja,
    precio,
    descripcion,
    sistema,
    imagen,
  } = req.body;

  try {
    let imageUrl = null;
    let imageFileId = null;

    // Si viene una nueva imagen en base64, subirla a ImageKit
    if (imagen && imagen.startsWith('data:image/')) {
      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({ error: 'Imagen base64 inválida.' });
      }

      const ext = matches[1];

      const uploadResult = await imagekit.upload({
        file: imagen,
        fileName: `dispositivo_${id}.${ext}`,
        folder: '/dispositivos',
        useUniqueFileName: false,
      });

      imageUrl = uploadResult.url;
      imageFileId = uploadResult.fileId;
    }

    // Armar la query de actualización
    let updateQuery = `
      UPDATE dispositivos SET
        nombre = $1,
        rareza = $2,
        nivel = $3,
        "costeVentaja" = $4,
        precio = $5,
        descripcion = $6,
        sistema = $7
    `;

    const updateValues = [
      nombre,
      rareza,
      nivel,
      costeVentaja,
      precio,
      descripcion,
      sistema,
    ];

    if (imageUrl) {
      updateQuery += `, imagenurl = $8, imagenfileid = $9`;
      updateValues.push(imageUrl, imageFileId);
    }

    updateQuery += ` WHERE idobjeto = $${updateValues.length + 1}`;
    updateValues.push(id);

    await pool.query(updateQuery, updateValues);

    res.json({
      message: 'Dispositivo Neotecnico actualizado exitosamente.'
    });

  } catch (err) {
    console.error('Error al actualizar dispositivo Neotecnico:', err);
    res.status(500).json({
      error: 'Error al actualizar Dispositivo Neotecnico.'
    });
  }
});


//ok!!
app.delete('/deleteObjetoNeo/:idobjeto', async (req, res) => {
  const { idobjeto } = req.params;
    //console.log('Intentando eliminar objeto con id:', idobjeto);

  try {
    // Opcional: verificar que el objeto exista antes de eliminar

    const deleteQuery = 'DELETE FROM dispositivos WHERE idobjeto = $1';
    const result = await pool.query(deleteQuery, [idobjeto]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Dispositivo Neotecnico no encontrado.' });
    }

    res.json({ message: 'Dispositivo Neotecnico eliminado correctamente.' });
  } catch (error) {
    console.error('Error al eliminar Dispositivo Neotecnico:', error);
    res.status(500).json({ error: 'Error al eliminar Dispositivo Neotecnico.' });
  }
});

//******************************************************************************** 







//****************************** Herbolaria App MOBILE  ************************************
app.get('/consumirObjetosH', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT *
      FROM elementos
    `);

    res.status(200).json({ objetosH: result.rows });
  } catch (error) {
    console.error('Error al consumir elementos de Herbolaria:', error);
    res.status(500).json({ error: 'Error interno al obtener elementos de Herbolaria' });
  }
});


app.post('/insertObjetoH', async (req, res) => {

  console.log(" red : ", req.body);

  const {
    nombre, rareza, nivel, costeVentaja, precio, descripcion, sistema, imagen
  } = req.body;

  try {
    // 1. Insertar elemento sin imagen
    const query = `
      INSERT INTO elementos (
        nombre, rareza, nivel, "costeVentaja", precio, descripcion, sistema
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7
      )
      RETURNING idobjeto
    `;

    const values = [
      nombre, rareza, nivel, costeVentaja, precio, descripcion, sistema
    ];

    const result = await pool.query(query, values);
    const newId = result.rows[0].idobjeto;

    let imageUrl = null;

    // 2. Subir imagen a ImageKit
    if (imagen) {
      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({
          error: 'Imagen base64 inválida.'
        });
      }

      const ext = matches[1];

      const uploadResult = await subirImagenImageKit(
        imagen,
        'elementos',
        `elemento_${newId}.${ext}`
      );

      imageUrl = uploadResult.imagenurl;
      const imageCloudId = uploadResult.imagencloudid;

      // 3. Actualizar URL e ID de ImageKit en PostgreSQL
      await pool.query(
        `UPDATE elementos
         SET imagenurl = $1, imagencloudid = $2
         WHERE idobjeto = $3`,
        [imageUrl, imageCloudId, newId]
      );
    }

    // 4. Responder
    res.status(201).json({
      message: 'elemento de Herbolaria creado exitosamente.',
      idobjeto: newId,
      imagenurl: imageUrl,
    });

  } catch (err) {
    console.error('Error al insertar elemento de Herbolaria nuevo:', err);

    res.status(500).json({
      error: 'Error al insertar elemento de Herbolaria nuevo.'
    });
  }
});

//ok!!

app.put('/updateObjetoH/:id', async (req, res) => {
  const { id } = req.params;
  const {
    nombre,
    rareza,
    nivel,
    costeVentaja,
    precio,
    descripcion,
    sistema,
    imagen,
  } = req.body;

  try {
    let imageUrl = null;
    let imageCloudId = null;

    // 🔹 Si viene una nueva imagen, subirla a ImageKit
    if (imagen && imagen.startsWith('data:image/')) {

      const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

      if (!matches) {
        return res.status(400).json({ error: 'Imagen base64 inválida.' });
      }

      const ext = matches[1];

      const uploadResult = await subirImagenImageKit(
        imagen,
        'elementos',
        `elemento_${id}.${ext}`
      );

      imageUrl = uploadResult.imagenurl;
      imageCloudId = uploadResult.imagencloudid;
    }

    // 🔹 Armar query de actualización
    let updateQuery = `
      UPDATE elementos SET
        nombre = $1,
        rareza = $2,
        nivel = $3,
        "costeVentaja" = $4,
        precio = $5,
        descripcion = $6,
        sistema = $7
    `;

    const updateValues = [
      nombre,
      rareza,
      nivel,
      costeVentaja,
      precio,
      descripcion,
      sistema,
    ];

    // 🔹 Actualizar imagen solamente si se subió una nueva
    if (imageUrl) {
      updateQuery += `, imagenurl = $8, imagencloudid = $9`;
      updateValues.push(imageUrl, imageCloudId);
    }

    updateQuery += ` WHERE idobjeto = $${updateValues.length + 1}`;
    updateValues.push(id);

    await pool.query(updateQuery, updateValues);

    res.json({
      message: 'Elemento de Herbolaria actualizado exitosamente.'
    });

  } catch (err) {
    console.error('Error al actualizar elemento de Herbolaria:', err);

    res.status(500).json({
      error: 'Error al actualizar elemento de Herbolaria.'
    });
  }
});


//ok!!
app.delete('/deleteObjetoH/:idobjeto', async (req, res) => {
  const { idobjeto } = req.params;
    //console.log('Intentando eliminar objeto con id:', idobjeto);

  try {
    // Opcional: verificar que el objeto exista antes de eliminar

    const deleteQuery = 'DELETE FROM elementos WHERE idobjeto = $1';
    const result = await pool.query(deleteQuery, [idobjeto]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Elemento de Herbolaria no encontrado.' });
    }

    res.json({ message: 'Elemento de Herbolaria eliminado correctamente.' });
  } catch (error) {
    console.error('Error al eliminar elemento de Herbolaria:', error);
    res.status(500).json({ error: 'Error al eliminar elemento de Herbolaria.' });
  }
});
//***************************************************************** 




//************************Historial DEL CHAT ***************************** */
app.get('/buscarHistorialPj', async (req, res) => {
  const { nombre } = req.query; // o req.body si es POST
  //console.log("lo que viene ", req.query)

  try {
    const result = await pool.query(
      `SELECT * FROM (
         SELECT * FROM mensajes
         WHERE REPLACE(LOWER(nombre), ' ', '') = REPLACE(LOWER($1), ' ', '')
         ORDER BY id DESC
         LIMIT 100
       ) sub
       ORDER BY id ASC`,
      [nombre]
    );

    res.status(200).json({ historialPj: result.rows });
  } catch (error) {
    console.error('Error al consumir historial de personaje:', error);
    res.status(500).json({ error: 'Error interno al obtener historial de personaje' });
  }
});


app.get('/pedirHistorialKen', async (req, res) => {
  const { idpersonaje } = req.query; // viene del GET ?idpersonaje=123

  try {
    const result = await pool.query(
      `SELECT * FROM mensajes
       WHERE "idpersonajeReceptor" = $1  -- solo los mensajes donde este pj recibió Ken
         AND tipo = 'entregaKen'
       ORDER BY id DESC`, // ASC para orden cronológico
      [idpersonaje]
    );

    res.status(200).json({ historialKen: result.rows });
  
  } catch (error) {
    console.error('Error al consumir historial de ken del personaje:', error);
    res.status(500).json({ error: 'Error interno al obtener historial de ken del personaje' });
  }
});
//**************************************************************





//****************************** COLECCION DE IMAGENES ***************************** */

app.put('/agregarImagenColeccion/:id', async (req, res) => {
  const idpersonaje = req.params.id;
  const { imagen } = req.body;

  console.log("ID del personaje para agregar imagen a coleccion:", idpersonaje);
  console.log("Imagen recibida para agregar a coleccion:", imagen ? "Sí" : "No");

  try {
    if (!imagen || !imagen.startsWith('data:image/')) {
      return res.status(400).json({ error: 'Imagen base64 inválida.' });
    }

    // 🔹 Validar formato de imagen
    const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

    if (!matches) {
      return res.status(400).json({ error: 'Formato de imagen inválido.' });
    }

    const ext = matches[1];

    // 🔹 Verificar que exista el personaje
    const { rows } = await pool.query(
      'SELECT "coleccionImagenes" FROM personajes WHERE idpersonaje = $1',
      [idpersonaje]
    );

    if (!rows[0]) {
      return res.status(404).json({ error: 'Personaje no encontrado.' });
    }

    // 🔹 Recuperar colección actual
    let coleccion = rows[0].coleccionImagenes || [];

    coleccion = typeof coleccion === 'string'
      ? JSON.parse(coleccion)
      : coleccion;

    // 🔹 Subir imagen a ImageKit
    const uploadResult = await subirImagenImageKit(
      imagen,
      'personajes',
      `personaje_${idpersonaje}_${Date.now()}.${ext}`
    );

    const imagenurl = uploadResult.imagenurl;
    const cloudid = uploadResult.imagencloudid;

    // 🔹 Crear objeto de la nueva imagen
    const nuevaImagen = {
      id: cloudid,
      url: imagenurl
    };

    // 🔹 Agregar imagen a la colección
    coleccion.push(nuevaImagen);

    // 🔹 Guardar colección actualizada
    await pool.query(
      'UPDATE personajes SET "coleccionImagenes" = $1 WHERE idpersonaje = $2',
      [JSON.stringify(coleccion), idpersonaje]
    );

    // 🔹 Respuesta
    res.status(201).json({
      message: 'Imagen agregada a la colección.',
      coleccion
    });

  } catch (err) {
    console.error('Error al agregar imagen a la colección:', err.message);

    res.status(500).json({
      error: 'Error al agregar imagen a la colección.'
    });
  }
});



app.put('/cambiarImagenColeccion/:id', async (req, res) => {
  const idpersonaje = req.params.id;
  const { imagenId, nuevaImagen } = req.body;

  console.log(
    "ID personaje:", idpersonaje,
    "Imagen recibida:", nuevaImagen ? "Sí" : "No",
    "Imagen a reemplazar ID:", imagenId
  );

  try {
    if (!nuevaImagen || !nuevaImagen.startsWith('data:image/')) {
      return res.status(400).json({ error: 'Imagen base64 inválida.' });
    }

    if (!imagenId) {
      return res.status(400).json({ error: 'Debe enviarse el ID de la imagen a reemplazar.' });
    }

    // Validar formato de imagen
    const matches = nuevaImagen.match(/^data:image\/(\w+);base64,(.+)$/);

    if (!matches) {
      return res.status(400).json({ error: 'Formato de imagen inválido.' });
    }

    const ext = matches[1];

    // Traer colección actual
    const { rows } = await pool.query(
      'SELECT "coleccionImagenes" FROM personajes WHERE idpersonaje = $1',
      [idpersonaje]
    );

    if (!rows[0]) {
      return res.status(404).json({ error: 'Personaje no encontrado.' });
    }

    let coleccion = rows[0].coleccionImagenes || [];
    coleccion = typeof coleccion === 'string'
      ? JSON.parse(coleccion)
      : coleccion;

    // Buscar imagen a reemplazar
    const index = coleccion.findIndex(img => img.id === imagenId);

    if (index === -1) {
      return res.status(404).json({ error: 'Imagen no encontrada en la colección.' });
    }

    // Subir nueva imagen a ImageKit
    const uploadResult = await subirImagenImageKit(
      nuevaImagen,
      'personajes',
      `personaje_${idpersonaje}_${Date.now()}.${ext}`
    );

    const imagenurl = uploadResult.imagenurl;
    const cloudid = uploadResult.imagencloudid;

    // Borrar imagen anterior de ImageKit
    try {
      await imagekit.deleteFile(imagenId);
    } catch (err) {
      console.warn(
        'No se pudo borrar la imagen vieja de ImageKit:',
        err.message
      );
    }

    // Reemplazar imagen en la colección
    coleccion[index] = {
      id: cloudid,
      url: imagenurl
    };

    // Actualizar base de datos
    await pool.query(
      `UPDATE personajes
       SET "coleccionImagenes" = $1,
           imagenurl = $2,
           imagencloudid = $3,
           "imagenSeleccionada" = $3
       WHERE idpersonaje = $4`,
      [
        JSON.stringify(coleccion),
        imagenurl,
        cloudid,
        idpersonaje
      ]
    );

    // Respuesta
    res.status(200).json({
      message: 'Imagen reemplazada correctamente.',
      coleccion,
      nuevaImagenId: cloudid
    });

  } catch (err) {
    console.error(
      'Error al reemplazar imagen en la colección:',
      err.message
    );

    res.status(500).json({
      error: 'Error al reemplazar imagen en la colección.'
    });
  }
});
;

//la coleccion guarda imagenes en kit

app.put('/eliminarImagenColeccion/:id', async (req, res) => {
  const idpersonaje = req.params.id;
  const { imagenId } = req.body;

  console.log("ID personaje:", idpersonaje);
  console.log("Imagen a eliminar:", imagenId);

  try {
    if (!imagenId) {
      return res.status(400).json({ error: 'Debe enviarse el ID de la imagen.' });
    }

    // Traer datos actuales
    const { rows } = await pool.query(
      'SELECT "coleccionImagenes", "imagenSeleccionada" FROM personajes WHERE idpersonaje = $1',
      [idpersonaje]
    );

    if (!rows[0]) {
      return res.status(404).json({ error: 'Personaje no encontrado.' });
    }

    let coleccion = rows[0].coleccionImagenes || [];
    const imagenSeleccionada = rows[0].imagenSeleccionada;

    coleccion = typeof coleccion === 'string'
      ? JSON.parse(coleccion)
      : coleccion;

    // Buscar imagen a eliminar
    const index = coleccion.findIndex(img => img.id === imagenId);

    if (index === -1) {
      return res.status(404).json({ error: 'Imagen no encontrada en la colección.' });
    }

    // Borrar de ImageKit
    try {
      await imagekit.deleteFile(coleccion[index].id);
    } catch (err) {
      console.warn('No se pudo borrar de ImageKit:', err.message);
    }

    // Eliminar de la colección
    coleccion.splice(index, 1);

    // Nueva imagen seleccionada
    let nuevaSeleccion = null;

    if (coleccion.length > 0) {
      if (imagenSeleccionada === imagenId) {
        nuevaSeleccion = coleccion[0];
      } else {
        nuevaSeleccion = coleccion.find(img => img.id === imagenSeleccionada) || coleccion[0];
      }
    }

    // Guardar en DB
    await pool.query(
      `UPDATE personajes 
       SET "coleccionImagenes" = $1,
           imagenurl = $2,
           imagencloudid = $3,
           "imagenSeleccionada" = $3
       WHERE idpersonaje = $4`,
      [
        JSON.stringify(coleccion),
        nuevaSeleccion ? nuevaSeleccion.url : null,
        nuevaSeleccion ? nuevaSeleccion.id : null,
        idpersonaje
      ]
    );

    // Respuesta
    res.status(200).json({
      message: 'Imagen eliminada correctamente.',
      coleccion,
      imagenSeleccionada: nuevaSeleccion ? nuevaSeleccion.id : null,
      imagenurl: nuevaSeleccion ? nuevaSeleccion.url : null
    });

  } catch (err) {
    console.error('Error al eliminar imagen:', err.message);
    res.status(500).json({ error: 'Error al eliminar imagen.' });
  }
});


app.put('/seleccionarImagen/:id', async (req, res) => {

    const idpersonaje = req.params.id;

    const { imagenSeleccionada, imagenurl } = req.body;


    console.log("====== SELECCIONAR IMAGEN ======");
    console.log("ID personaje:", idpersonaje);
    console.log("imagenSeleccionada:", imagenSeleccionada);
    console.log("imagenurl:", imagenurl);
    console.log("Body completo:", req.body);


    try {

        const resultado = await pool.query(
            `UPDATE personajes
             SET "imagenSeleccionada" = $1,
                 imagenurl = $2
             WHERE idpersonaje = $3`,
            [
                imagenSeleccionada,
                imagenurl,
                idpersonaje
            ]
        );


        console.log("Filas modificadas:", resultado.rowCount);


        res.json({ 
            mensaje: "Imagen seleccionada actualizada",
            filas: resultado.rowCount
        });


    } catch (error) {

        console.error("ERROR seleccionando imagen:", error.message);

        res.status(500).json({
            error: error.message
        });
    }
});

//*********************************************************************************** */





//***************************************************************************** */
//******************************APLICACION DE ESCRITORIO ********************** */


//**********************************ITEMS******************************************* */
app.get('/ConsumirItemsInventario/:idPersonaje', async (req, res) => {
    const idPersonaje = req.params.idPersonaje;

    try {
        const result = await pool.query(
            `SELECT *
             FROM items
             WHERE idpersonaje = $1`,
            [idPersonaje]
        );

        return res.json(result.rows);
    } catch (error) {
        console.log(error);
        return res.status(500).json({ error: error.message });
    }
});

app.get('/ConsumirItemsManual', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT *
            FROM tesoros
        `);

        return res.json(result.rows);


    }
    catch (error) {
        console.log(error);
        return res.status(500).json({ error: error.message });
    }
});


app.post('/insertItem', async (req, res) => {

  const nombre = req.body.Nombre || req.body.nombre || "Ítem sin nombre";
  const tipo = req.body.Tipo || req.body.tipo || "";
  const rareza = req.body.Rareza || req.body.rareza || "";
  const cantidad = req.body.Cantidad || req.body.cantidad || 1;

  const idpersonaje = req.body.PersonajeId || req.body.idpersonaje;

  const descripcion = req.body.descripcion || req.body.Descripcion || "";
  const sistema = req.body.Sistema || req.body.sistema || "";
  const precio = String(req.body.precio ?? req.body.Precio ?? "");
  const imagen = req.body.Imagen_url || null;
  const posicion = req.body.Posicion ?? req.body.posicion ?? null;

  const contenedor = req.body.Contenedor ?? req.body.contenedor ?? null;
  const efecto = req.body.Efecto ?? req.body.efecto ?? null;

  if (!idpersonaje) {
    return res.status(400).json({ error: 'Falta idpersonaje' });
  }

  const clientDb = await pool.connect();

  try {
    await clientDb.query('BEGIN');

    const queryItem = `
      INSERT INTO items (
        nombre, tipo, descripcion, sistema,
        precio, rareza, cantidad,
        posicion, contenedor, efecto, idpersonaje
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
      RETURNING id
    `;

    const resultItem = await clientDb.query(queryItem, [
      nombre,
      tipo,
      descripcion,
      sistema,
      precio,
      rareza,
      cantidad,
      posicion,
      contenedor,
      efecto,
      idpersonaje
    ]);

    const newId = resultItem.rows[0].id;

    let imageUrl = null;

    if (imagen) {
      if (imagen.startsWith('http')) {
        imageUrl = imagen;

        await clientDb.query(
          'UPDATE items SET imagen_url = $1 WHERE id = $2',
          [imageUrl, newId]
        );

      } else {
        const matches = imagen.match(/^data:image\/(\w+);base64,(.+)$/);

        if (matches) {
          const ext = matches[1];

          const uploadResult = await subirImagenImageKit(
            imagen,
            'items',
            `item_${newId}.${ext}`
          );

          imageUrl = uploadResult.imagenurl;
          const imageFileId = uploadResult.imagencloudid;

          await clientDb.query(
            'UPDATE items SET imagen_url = $1, imagen_cloud_id = $2 WHERE id = $3',
            [imageUrl, imageFileId, newId]
          );
        }
      }
    }

    await clientDb.query('COMMIT');

    res.status(201).json({
      message: 'Item creado correctamente',
      iditem: newId,
      idpersonaje,
      imagen_url: imageUrl
    });

  } catch (err) {
    await clientDb.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: 'Error interno al insertar item' });
  } finally {
    clientDb.release();
  }
});



app.put('/ActualizarPosicionItem', async (req, res) => {
    try {
        const { ItemId, Posicion, Contenedor } = req.body;

        if (!ItemId) {
            return res.status(400).json({ error: 'ItemId requerido' });
        }

        await pool.query(
            `
            UPDATE items
            SET posicion = $1,
                contenedor = $2
            WHERE id = $3
            `,
            [Posicion, Contenedor, ItemId]
        );

        return res.status(200).json({ message: 'OK' });

    } catch (error) {
        console.log(error);
        return res.status(500).json({ error: error.message });
    }
});



//los items estan gaurdados en imagenkit


app.delete('/deleteItem', async (req, res) => {

  const iditem = req.body.iditem || req.body.IdItem;

  console.log("ID del item a eliminar: ", iditem);

  if (!iditem) {
    return res.status(400).json({
      error: 'Falta iditem'
    });
  }

  const clientDb = await pool.connect();

  try {

    await clientDb.query('BEGIN');

    // 1. Obtener imagen del item
    const result = await clientDb.query(
      `
      SELECT imagen_cloud_id, imagen_url
      FROM items
      WHERE id = $1
      `,
      [iditem]
    );

    if (result.rows.length === 0) {
      await clientDb.query('ROLLBACK');

      return res.status(404).json({
        error: 'Item no encontrado'
      });
    }

    const { imagen_cloud_id } = result.rows[0];

    // 2. Eliminar imagen de ImageKit
    if (imagen_cloud_id) {

      try {

        console.log(
          `Item ${iditem}: eliminando imagen de ImageKit`
        );

        await imagekit.deleteFile(imagen_cloud_id);

      } catch (imageErr) {

        console.error(
          `Error eliminando imagen del item ${iditem}:`,
          imageErr
        );

      }
    }

    // 3. Eliminar item de PostgreSQL
    await clientDb.query(
      `
      DELETE FROM items
      WHERE id = $1
      `,
      [iditem]
    );

    await clientDb.query('COMMIT');

    res.status(200).json({
      message: 'Item eliminado correctamente',
      iditem
    });

  } catch (err) {

    await clientDb.query('ROLLBACK');

    console.error(
      'Error interno al eliminar item:',
      err
    );

    res.status(500).json({
      error: 'Error interno al eliminar item'
    });

  } finally {

    clientDb.release();
  }
});


app.put('/ActualizarCantidadItem', async (req, res) => {

    try {

        const { ItemId, Cantidad } = req.body;

       


        if (!ItemId) {
            return res.status(400).json({ error: 'ItemId requerido' });
        }


        if (Cantidad === undefined || Cantidad === null) {
            return res.status(400).json({ error: 'Cantidad requerida' });
        }


        await pool.query(
            `
            UPDATE items
            SET cantidad = $1
            WHERE id = $2
            `,
            [
                Cantidad,
                ItemId
            ]
        );


        return res.status(200).json({
            message: 'Cantidad actualizada correctamente'
        });


    } catch (error) {

        console.log(error);

        return res.status(500).json({
            error: error.message
        });
    }
});

app.get('/ConsumirItemsBiblioteca', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT *
            FROM items_biblioteca
        `);

        return res.json(result.rows);


    }
    catch (error) {
        console.log(error);
        return res.status(500).json({ error: error.message });
    }
});


app.post('/insertItemBiblioteca', async (req, res) => {

  const nombre = req.body.Nombre || req.body.nombre || "Ítem sin nombre";
  const tipo = req.body.Tipo || req.body.tipo || "";
  const rareza = req.body.Rareza || req.body.rareza || "";
  const nivel = req.body.Nivel || req.body.nivel || "";
  const descripcion = req.body.Descripcion || req.body.descripcion || "";
  const sistema = req.body.Sistema || req.body.sistema || "";
  const precio = req.body.Precio || req.body.precio || "";
  const costeVentaja = req.body.CosteVentaja || req.body.costeVentaja || "";
  const imagen = req.body.Imagen_url || req.body.imagen_url || null;
  const efecto = req.body.Efecto ?? req.body.efecto ?? "";

  const clientDb = await pool.connect();

  try {

    await clientDb.query('BEGIN');

    const queryItem = `
      INSERT INTO items_biblioteca (
        nombre,
        tipo,
        rareza,
        nivel,
        descripcion,
        sistema,
        precio,
        "costeVentaja",
        efecto
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING idobjeto
    `;

    const resultItem = await clientDb.query(queryItem, [
      nombre,
      tipo,
      rareza,
      nivel,
      descripcion,
      sistema,
      precio,
      costeVentaja,
      efecto
    ]);

    const newId = resultItem.rows[0].idobjeto;

    let imageUrl = null;
    let imageCloudId = null;

    if (imagen) {

      if (imagen.startsWith('http')) {

        imageUrl = imagen;

        await clientDb.query(
          `
          UPDATE items_biblioteca
          SET imagenurl = $1
          WHERE idobjeto = $2
          `,
          [imageUrl, newId]
        );

      } else {

        const uploadResult = await subirImagenImageKit(
          imagen,
          'items',
          `item_biblioteca_${newId}`
        );

        imageUrl = uploadResult.imagenurl;
        imageCloudId = uploadResult.imagencloudid;

        await clientDb.query(
          `
          UPDATE items_biblioteca
          SET imagenurl = $1,
              imagencloudid = $2
          WHERE idobjeto = $3
          `,
          [
            imageUrl,
            imageCloudId,
            newId
          ]
        );
      }
    }

    await clientDb.query('COMMIT');

    res.status(201).json({
      message: 'Item creado correctamente en la biblioteca',
      idobjeto: newId,
      imagenurl: imageUrl,
      imagencloudid: imageCloudId
    });

  } catch (err) {

    await clientDb.query('ROLLBACK');

    console.error(
      'Error al insertar item en biblioteca:',
      err
    );

    res.status(500).json({
      error: 'Error interno al insertar item en biblioteca'
    });

  } finally {

    clientDb.release();
  }
});

//****************************************************************************** */





//************************* ACIONES ************************************

app.get('/ConsumirListaAcciones/:idPersonaje', async (req, res) => {
    const idPersonaje = req.params.idPersonaje;

    //console.log(`[CONSUMIR LISTA ACCIONES SE DISPARO] Request recibida para personaje: ${idPersonaje}`);

    try {
        const result = await pool.query(
            `SELECT *
             FROM acciones
             WHERE idpersonaje = $1`,
            [idPersonaje]
        );

       // console.log(`[CONSUMIR LISTA ACCIONES] OK personaje ${idPersonaje} - acciones encontradas: ${result.rows.length}`);

        return res.json(result.rows);
    } catch (error) {
        console.log(error);
        return res.status(500).json({ error: error.message });
    }
});



//INSERT DE ACCIONES INICALES DE UN PERSONAJE NUEVO
app.post('/crearAccionesIniciales', async (req, res) => {

    const idPersonaje = req.body.idPersonaje;
    const idsAcciones = req.body.idsAcciones;

    if (!idPersonaje) {
        return res.status(400).json({
            error: 'Falta idPersonaje'
        });
    }

    if (!Array.isArray(idsAcciones) || idsAcciones.length === 0) {
        return res.status(400).json({
            error: 'Faltan idsAcciones'
        });
    }

    const clientDb = await pool.connect();

    try {

        await clientDb.query('BEGIN');

        const query = `
            INSERT INTO acciones (
                nombre,
                tipo,
                descripcion,
                sistema,
                rareza,
                posicion,
                contenedor,
                efecto,
                idpersonaje,
                prioridad,
                dominio,
                arte,
                ryu,
                tiempo_invocacion,
                nivel_ki,
                coste_ki,
                imagen_url
            )
            SELECT
                ab.nombre,
                ab.tipo,
                ab.descripcion,
                ab.sistema,
                ab.rareza,
                ROW_NUMBER() OVER (ORDER BY ab.id) - 1,
                'ACCIONES',
                ab.efecto,
                $1,
                ab.prioridad,
                ab.dominio,
                ab.arte,
                ab.ryu,
                ab.tiempo_invocacion,
                ab.nivel_ki,
                ab.coste_ki,
                ab.imagen_url
            FROM acciones_biblioteca ab
            WHERE ab.id = ANY($2::int[])
            ORDER BY ab.id
            RETURNING id;
        `;

        const resultado = await clientDb.query(
            query,
            [
                idPersonaje,
                idsAcciones
            ]
        );

        await clientDb.query('COMMIT');

        res.status(201).json({
            message: 'Acciones iniciales creadas correctamente',
            idPersonaje: idPersonaje,
            cantidad: resultado.rows.length,
            idsAccionesCreadas: resultado.rows.map(row => row.id)
        });

    } catch (err) {

        await clientDb.query('ROLLBACK');

        console.error(err);

        res.status(500).json({
            error: 'Error interno al crear las acciones iniciales'
        });

    } finally {

        clientDb.release();
    }
});


app.post('/insertAccion', async (req, res) => {

  const nombre = req.body.Nombre || req.body.nombre || "Accion sin nombre";
  const tipo = req.body.Tipo || req.body.tipo || "";
  const rareza = req.body.Rareza || req.body.rareza || "";
  //const cantidad = req.body.Cantidad || req.body.cantidad || 1;

  const idpersonaje = req.body.PersonajeId || req.body.idpersonaje;

  const descripcion = req.body.descripcion || req.body.Descripcion || "";
  const sistema = req.body.Sistema || req.body.sistema || "";
  //const precio = req.body.precio || req.body.Precio || 0;
  const imagen = req.body.Imagen_url || null;
  const posicion = req.body.Posicion ?? req.body.posicion ?? null;
  const contenedor = req.body.Contenedor || req.body.contenedor || null;

  const efecto = req.body.Efecto || req.body.efecto || null;

  // Campos específicos de tipo de acción
  const prioridad = req.body.Prioridad || "";
  const dominio = req.body.Dominio || "";
  const arte = req.body.Arte || "";
  const ryu = req.body.Ryu || "";
  const tiempo_invocacion = req.body.Tiempo_invocacion || "";
  const nivel_ki = req.body.Nivel_ki ?? 0;
  const coste_ki = req.body.Coste_ki ?? 0;

  if (!idpersonaje) {
    return res.status(400).json({ error: 'Falta idpersonaje' });
  }

  const clientDb = await pool.connect();

  try {
    await clientDb.query('BEGIN');

    const queryItem = `
      INSERT INTO acciones (
        nombre,
        tipo,
        descripcion,
        sistema,
        rareza,
        posicion,
        contenedor,
        efecto,
        idpersonaje,
        prioridad,
        dominio,
        arte,
        ryu,
        tiempo_invocacion,
        nivel_ki,
        coste_ki
      )
      VALUES (
        $1,$2,$3,$4,$5,
        $6,$7,$8,$9,
        $10,$11,$12,$13,$14,$15,$16
      )
      RETURNING id
    `;

    const resultItem = await clientDb.query(queryItem, [
      nombre,
      tipo,
      descripcion,
      sistema,
      rareza,
      posicion,
      contenedor,
      efecto,
      idpersonaje,
      prioridad,
      dominio,
      arte,
      ryu,
      tiempo_invocacion,
      nivel_ki,
      coste_ki
    ]);

    const newId = resultItem.rows[0].id;

    let imageUrl = null;
    let imageCloudId = null;

    if (imagen) {

      if (imagen.startsWith('http')) {

        imageUrl = imagen;

        await clientDb.query(
          'UPDATE acciones SET imagen_url = $1 WHERE id = $2',
          [imageUrl, newId]
        );

      } else {

        const uploadResult = await subirImagenImageKit(
          imagen,
          'acciones',
          `accion_${newId}`
        );

        imageUrl = uploadResult.imagenurl;
        imageCloudId = uploadResult.imagencloudid;

        await clientDb.query(
          'UPDATE acciones SET imagen_url = $1, imagen_cloud_id = $2 WHERE id = $3',
          [imageUrl, imageCloudId, newId]
        );
      }
    }

    await clientDb.query('COMMIT');

    res.status(201).json({
      message: 'Accion creada correctamente',
      iditem: newId,
      idpersonaje,
      imagen_url: imageUrl
    });

  } catch (err) {
    await clientDb.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: 'Error interno al insertar accion' });
  } finally {
    clientDb.release();
  }
});



app.post('/insertAccionBiblioteca', async (req, res) => {

  const nombre = req.body.Nombre || req.body.nombre || "Accion sin nombre";
  const tipo = req.body.Tipo || req.body.tipo || "";
  const rareza = req.body.Rareza || req.body.rareza || "";

  const descripcion = req.body.descripcion || req.body.Descripcion || "";
  const sistema = req.body.Sistema || req.body.sistema || "";

  const imagen = req.body.Imagen_url || null;
  const efecto = req.body.Efecto || req.body.efecto || null;

  const prioridad = req.body.Prioridad || "";
  const dominio = req.body.Dominio || "";
  const arte = req.body.Arte || "";
  const ryu = req.body.Ryu || "";
  const tiempo_invocacion = req.body.Tiempo_invocacion || "";
  const nivel_ki = req.body.Nivel_ki ?? 0;
  const coste_ki = req.body.Coste_ki ?? 0;

  const clientDb = await pool.connect();

  try {

    await clientDb.query('BEGIN');

    const queryAccion = `
      INSERT INTO acciones_biblioteca (
        nombre,
        tipo,
        descripcion,
        sistema,
        rareza,
        efecto,
        prioridad,
        dominio,
        arte,
        ryu,
        tiempo_invocacion,
        nivel_ki,
        coste_ki
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,
        $7,$8,$9,$10,$11,$12,$13
      )
      RETURNING id
    `;

    const resultAccion = await clientDb.query(queryAccion, [
      nombre,
      tipo,
      descripcion,
      sistema,
      rareza,
      efecto,
      prioridad,
      dominio,
      arte,
      ryu,
      tiempo_invocacion,
      nivel_ki,
      coste_ki
    ]);

    const newId = resultAccion.rows[0].id;

    let imageUrl = null;
    let imageCloudId = null;

    if (imagen) {

      if (imagen.startsWith('http')) {

        imageUrl = imagen;

        await clientDb.query(
          'UPDATE acciones_biblioteca SET imagen_url = $1 WHERE id = $2',
          [imageUrl, newId]
        );

      } else {

        const uploadResult = await subirImagenImageKit(
          imagen,
          'acciones_biblioteca',
          `accion_biblioteca_${newId}`
        );

        imageUrl = uploadResult.imagenurl;
        imageCloudId = uploadResult.imagencloudid;

        await clientDb.query(
          `
          UPDATE acciones_biblioteca
          SET imagen_url = $1,
              imagen_cloud_id = $2
          WHERE id = $3
          `,
          [
            imageUrl,
            imageCloudId,
            newId
          ]
        );
      }
    }

    await clientDb.query('COMMIT');

    res.status(201).json({
      message: 'Accion guardada en biblioteca correctamente',
      idaccion: newId,
      imagen_url: imageUrl
    });

  } catch (err) {

    await clientDb.query('ROLLBACK');

    console.error(err);

    res.status(500).json({
      error: 'Error interno al insertar accion en biblioteca'
    });

  } finally {

    clientDb.release();

  }

});


app.get('/ConsumirAccionesBiblioteca', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT *
            FROM acciones_biblioteca
        `);


      
        return res.json(result.rows);


    }
    catch (error) {
        console.log(error);
        return res.status(500).json({ error: error.message });
    }
});

app.put('/ActualizarPosicionAccion', async (req, res) => {
    try {
        const { ItemId, Posicion, Contenedor } = req.body;

      

        if (!ItemId) {
            return res.status(400).json({ error: 'ItemId requerido' });
        }

        await pool.query(
            `
            UPDATE acciones
            SET posicion = $1,
                contenedor = $2
            WHERE id = $3
            `,
            [Posicion, Contenedor, ItemId]
        );

        return res.status(200).json({ message: 'OK' });

    } catch (error) {
        console.log(error);
        return res.status(500).json({ error: error.message });
    }
});


//las acciones estan guardadas en imagenkit

app.delete('/deleteAccion', async (req, res) => {

  const idaccion = req.body.idaccion || req.body.IdAccion;

  if (!idaccion) {
    return res.status(400).json({
      error: 'Falta idaccion'
    });
  }

  const clientDb = await pool.connect();

  try {

    await clientDb.query('BEGIN');

    // 1. Obtener imagen de la acción
    const result = await clientDb.query(
      `
      SELECT imagen_cloud_id, imagen_url
      FROM acciones
      WHERE id = $1
      `,
      [idaccion]
    );

    if (result.rows.length === 0) {

      await clientDb.query('ROLLBACK');

      return res.status(404).json({
        error: 'Accion no encontrada'
      });
    }

    const { imagen_cloud_id, imagen_url } = result.rows[0];

    // 2. Eliminar imagen de ImageKit
    if (
      imagen_cloud_id &&
      imagen_url &&
      imagen_url.includes('ik.imagekit.io/')
    ) {

      try {

        console.log(
          `🟢 Acción ${idaccion}: eliminando imagen de ImageKit`
        );

        await imagekit.files.delete(imagen_cloud_id);

      } catch (imageErr) {

        console.error(
          `⚠️ Error eliminando imagen de acción ${idaccion}:`,
          imageErr
        );

      }
    }

    // 3. Eliminar acción de PostgreSQL
    await clientDb.query(
      `
      DELETE FROM acciones
      WHERE id = $1
      `,
      [idaccion]
    );

    await clientDb.query('COMMIT');

    res.status(200).json({
      message: 'Accion eliminada correctamente',
      idaccion
    });

  } catch (err) {

    await clientDb.query('ROLLBACK');

    console.error(
      'Error interno al eliminar accion:',
      err
    );

    res.status(500).json({
      error: 'Error interno al eliminar accion'
    });

  } finally {

    clientDb.release();

  }
});




//*************************************************************************** */





//*********************VENTAJAS*****************************************


app.get('/ConsumirVentajasDesventajasTodas', async (req, res) => {

    try {

        const result = await pool.query(
            `SELECT *
             FROM ventajas`
        );

        //console.log("Cantidad de ventajas:", result.rows.length);

        return res.json({
            message: "Consulta exitosa",
            cantidad: result.rows.length,
            coleccionVentajas: result.rows
        });

    } catch (error) {

        console.log(error);
        return res.status(500).json({ error: error.message });
    }
});

app.get('/ConsumirIdsVentajasPersonaje/:idPersonaje', async (req, res) => {
    const idPersonaje = req.params.idPersonaje;

    

    try {
        const result = await pool.query(
            `SELECT idventaja_fk
             FROM personajes_ventajas
             WHERE idpersonaje_fk = $1`,
            [idPersonaje]
        );

        const idsVentajas = result.rows.map(v => v.idventaja_fk);

       // console.log("Ventajas encontradas:", idsVentajas);

        return res.json(idsVentajas);

    } catch (error) {
        console.log(error);
        return res.status(500).json({ error: error.message });
    }
});

app.post('/AgregarVentajaPersonaje', async (req, res) => {

  const idPersonaje = req.body.idPersonaje || req.body.idpersonaje;
  const idVentaja = req.body.idVentaja || req.body.idventaja;

  if (!idPersonaje || !idVentaja) {
    return res.status(400).json({
      error: "Faltan datos."
    });
  }

  const clientDb = await pool.connect();

  try {

    await clientDb.query("BEGIN");

    const query = `
      INSERT INTO personajes_ventajas
      (
        idpersonaje_fk,
        idventaja_fk
      )
      VALUES
      (
        $1,
        $2
      )
      RETURNING idpersonajes_ventajas
    `;

    const resultado = await clientDb.query(query, [
      idPersonaje,
      idVentaja
    ]);

    await clientDb.query("COMMIT");

    res.status(201).json({
      message: "Ventaja agregada correctamente.",
      id: resultado.rows[0].id
    });

  }
  catch (err) {

    await clientDb.query("ROLLBACK");

    console.error(err);

    res.status(500).json({
      error: "Error al agregar la ventaja."
    });

  }
  finally {

    clientDb.release();

  }

});

app.post('/EliminarVentajaPersonaje', async (req, res) => {

  const idPersonaje = req.body.idPersonaje || req.body.idpersonaje;
  const idVentaja = req.body.idVentaja || req.body.idventaja;

  if (!idPersonaje || !idVentaja) {
    return res.status(400).json({
      error: 'Faltan idPersonaje o idVentaja'
    });
  }

  try {

    const resultado = await pool.query(
      `DELETE FROM personajes_ventajas
       WHERE idpersonaje_fk = $1
       AND idventaja_fk = $2`,
      [idPersonaje, idVentaja]
    );

    if (resultado.rowCount === 0) {
      return res.status(404).json({
        error: 'La ventaja no estaba asignada al personaje'
      });
    }

    res.status(200).json({
      message: 'Ventaja eliminada correctamente'
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: 'Error interno al eliminar la ventaja'
    });
  }

});


//.................INCREMENTOS PENDIENTES***********************

app.get('/ConsumirIncrementosPendientes/:idPersonaje', async (req, res) => {
    const idPersonaje = req.params.idPersonaje;

    

    try {
        const result = await pool.query(
            `SELECT *
             FROM incrementos_pendientes
             WHERE idpersonaje = $1`,
            [idPersonaje]
        );

        return res.json(result.rows);

    } catch (error) {
        console.log(error);

        return res.status(500).json({
            error: error.message
        });
    }
});

app.post('/GuardarIncremento', async (req, res) => {
    const incremento = req.body;

    // console.log("Incremento recibido:", incremento);

    try {
        await pool.query(
            `INSERT INTO incrementos_pendientes (
                idpersonaje, fuerza, fortaleza, destreza, agilidad, sabiduria, principio, presencia, sentidos,
                academisismo, alerta, atletismo, conbakemono, mentir, pilotear, artesmarciales, medicina,
                conobjmagicos, sigilo, conesferas, conleyendas, forja, condemonio, conespiritual, manejoblaster,
                manejosombras, tratobakemono, conhechiceria, medvital, medespiritual, rayo, fuego, frio,
                veneno, corte, energia, valcombate, valcombate2, valadd1, valadd2, valadd3, valadd4,
                ki
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
                $41, $42, 
                COALESCE($43, 0) -- ✅ Si el ki enviado desde C# llega nulo, guarda un 0
            )
            ON CONFLICT (idpersonaje)
            DO UPDATE SET
                fuerza = COALESCE(incrementos_pendientes.fuerza, 0) + COALESCE(EXCLUDED.fuerza, 0),
                fortaleza = COALESCE(incrementos_pendientes.fortaleza, 0) + COALESCE(EXCLUDED.fortaleza, 0),
                destreza = COALESCE(incrementos_pendientes.destreza, 0) + COALESCE(EXCLUDED.destreza, 0),
                agilidad = COALESCE(incrementos_pendientes.agilidad, 0) + COALESCE(EXCLUDED.agilidad, 0),
                sabiduria = COALESCE(incrementos_pendientes.sabiduria, 0) + COALESCE(EXCLUDED.sabiduria, 0),
                principio = COALESCE(incrementos_pendientes.principio, 0) + COALESCE(EXCLUDED.principio, 0),
                presencia = COALESCE(incrementos_pendientes.presencia, 0) + COALESCE(EXCLUDED.presencia, 0),
                sentidos = COALESCE(incrementos_pendientes.sentidos, 0) + COALESCE(EXCLUDED.sentidos, 0),

                academisismo = COALESCE(incrementos_pendientes.academisismo, 0) + COALESCE(EXCLUDED.academisismo, 0),
                alerta = COALESCE(incrementos_pendientes.alerta, 0) + COALESCE(EXCLUDED.alerta, 0),
                atletismo = COALESCE(incrementos_pendientes.atletismo, 0) + COALESCE(EXCLUDED.atletismo, 0),
                conbakemono = COALESCE(incrementos_pendientes.conbakemono, 0) + COALESCE(EXCLUDED.conbakemono, 0),
                mentir = COALESCE(incrementos_pendientes.mentir, 0) + COALESCE(EXCLUDED.mentir, 0),
                pilotear = COALESCE(incrementos_pendientes.pilotear, 0) + COALESCE(EXCLUDED.pilotear, 0),
                artesmarciales = COALESCE(incrementos_pendientes.artesmarciales, 0) + COALESCE(EXCLUDED.artesmarciales, 0),
                medicina = COALESCE(incrementos_pendientes.medicina, 0) + COALESCE(EXCLUDED.medicina, 0),
                conobjmagicos = COALESCE(incrementos_pendientes.conobjmagicos, 0) + COALESCE(EXCLUDED.conobjmagicos, 0),
                sigilo = COALESCE(incrementos_pendientes.sigilo, 0) + COALESCE(EXCLUDED.sigilo, 0),
                conesferas = COALESCE(incrementos_pendientes.conesferas, 0) + COALESCE(EXCLUDED.conesferas, 0),
                conleyendas = COALESCE(incrementos_pendientes.conleyendas, 0) + COALESCE(EXCLUDED.conleyendas, 0),
                forja = COALESCE(incrementos_pendientes.forja, 0) + COALESCE(EXCLUDED.forja, 0),
                condemonio = COALESCE(incrementos_pendientes.condemonio, 0) + COALESCE(EXCLUDED.condemonio, 0),
                conespiritual = COALESCE(incrementos_pendientes.conespiritual, 0) + COALESCE(EXCLUDED.conespiritual, 0),
                manejoblaster = COALESCE(incrementos_pendientes.manejoblaster, 0) + COALESCE(EXCLUDED.manejoblaster, 0),
                manejosombras = COALESCE(incrementos_pendientes.manejosombras, 0) + COALESCE(EXCLUDED.manejosombras, 0),
                tratobakemono = COALESCE(incrementos_pendientes.tratobakemono, 0) + COALESCE(EXCLUDED.tratobakemono, 0),
                conhechiceria = COALESCE(incrementos_pendientes.conhechiceria, 0) + COALESCE(EXCLUDED.conhechiceria, 0),

                medvital = COALESCE(incrementos_pendientes.medvital, 0) + COALESCE(EXCLUDED.medvital, 0),
                medespiritual = COALESCE(incrementos_pendientes.medespiritual, 0) + COALESCE(EXCLUDED.medespiritual, 0),
                rayo = COALESCE(incrementos_pendientes.rayo, 0) + COALESCE(EXCLUDED.rayo, 0),
                fuego = COALESCE(incrementos_pendientes.fuego, 0) + COALESCE(EXCLUDED.fuego, 0),
                frio = COALESCE(incrementos_pendientes.frio, 0) + COALESCE(EXCLUDED.frio, 0),
                veneno = COALESCE(incrementos_pendientes.veneno, 0) + COALESCE(EXCLUDED.veneno, 0),
                corte = COALESCE(incrementos_pendientes.corte, 0) + COALESCE(EXCLUDED.corte, 0),
                energia = COALESCE(incrementos_pendientes.energia, 0) + COALESCE(EXCLUDED.energia, 0),

                valcombate = COALESCE(incrementos_pendientes.valcombate, 0) + COALESCE(EXCLUDED.valcombate, 0),
                valcombate2 = COALESCE(incrementos_pendientes.valcombate2, 0) + COALESCE(EXCLUDED.valcombate2, 0),

                valadd1 = COALESCE(incrementos_pendientes.valadd1, 0) + COALESCE(EXCLUDED.valadd1, 0),
                valadd2 = COALESCE(incrementos_pendientes.valadd2, 0) + COALESCE(EXCLUDED.valadd2, 0),
                valadd3 = COALESCE(incrementos_pendientes.valadd3, 0) + COALESCE(EXCLUDED.valadd3, 0),
                valadd4 = COALESCE(incrementos_pendientes.valadd4, 0) + COALESCE(EXCLUDED.valadd4, 0),
                
                -- ✅ Blindamos el Ki contra sumas que involucren NULL
                ki = COALESCE(incrementos_pendientes.ki, 0) + COALESCE(EXCLUDED.ki, 0)
            `,
            [
                incremento.idpersonaje, incremento.fuerza, incremento.fortaleza, incremento.destreza,
                incremento.agilidad, incremento.sabiduria, incremento.principio, incremento.presencia,
                incremento.sentidos, incremento.academisismo, incremento.alerta, incremento.atletismo,
                incremento.conbakemono, incremento.mentir, incremento.pilotear, incremento.artesmarciales,
                incremento.medicina, incremento.conobjmagicos, incremento.sigilo, incremento.conesferas,
                incremento.conleyendas, incremento.forja, incremento.condemonio, incremento.conespiritual,
                incremento.manejoblaster, incremento.manejosombras, incremento.tratobakemono,
                incremento.conhechiceria, incremento.medvital, incremento.medespiritual, incremento.rayo,
                incremento.fuego, incremento.frio, incremento.veneno, incremento.corte, incremento.energia,
                incremento.valcombate, incremento.valcombate2, incremento.valadd1, incremento.valadd2,
                incremento.valadd3, incremento.valadd4, incremento.ki
            ]
        );

        return res.json({
            ok: true,
            mensaje: "Incremento guardado correctamente"
        });

    } catch (error) {
        console.log(error);
        return res.status(500).json({
            ok: false,
            error: error.message
        });
    }
});

app.post('/GuardarIncrementoNarrador', async (req, res) => {
    const incremento = req.body;

    //console.log("Incremento recibido del narrador:", incremento);

    try {
        await pool.query(
            `INSERT INTO incrementos_pendientes (
                idpersonaje, fuerza, fortaleza, destreza, agilidad, sabiduria, principio, presencia, sentidos,
                academisismo, alerta, atletismo, conbakemono, mentir, pilotear, artesmarciales, medicina,
                conobjmagicos, sigilo, conesferas, conleyendas, forja, condemonio, conespiritual, manejoblaster,
                manejosombras, tratobakemono, conhechiceria, medvital, medespiritual, rayo, fuego, frio,
                veneno, corte, energia, valcombate, valcombate2, valadd1, valadd2, valadd3, valadd4
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
                $41, $42
            )
            ON CONFLICT (idpersonaje)
            DO UPDATE SET
                fuerza = COALESCE(incrementos_pendientes.fuerza, 0) + COALESCE(EXCLUDED.fuerza, 0),
                fortaleza = COALESCE(incrementos_pendientes.fortaleza, 0) + COALESCE(EXCLUDED.fortaleza, 0),
                destreza = COALESCE(incrementos_pendientes.destreza, 0) + COALESCE(EXCLUDED.destreza, 0),
                agilidad = COALESCE(incrementos_pendientes.agilidad, 0) + COALESCE(EXCLUDED.agilidad, 0),
                sabiduria = COALESCE(incrementos_pendientes.sabiduria, 0) + COALESCE(EXCLUDED.sabiduria, 0),
                principio = COALESCE(incrementos_pendientes.principio, 0) + COALESCE(EXCLUDED.principio, 0),
                presencia = COALESCE(incrementos_pendientes.presencia, 0) + COALESCE(EXCLUDED.presencia, 0),
                sentidos = COALESCE(incrementos_pendientes.sentidos, 0) + COALESCE(EXCLUDED.sentidos, 0),

                academisismo = COALESCE(incrementos_pendientes.academisismo, 0) + COALESCE(EXCLUDED.academisismo, 0),
                alerta = COALESCE(incrementos_pendientes.alerta, 0) + COALESCE(EXCLUDED.alerta, 0),
                atletismo = COALESCE(incrementos_pendientes.atletismo, 0) + COALESCE(EXCLUDED.atletismo, 0),
                conbakemono = COALESCE(incrementos_pendientes.conbakemono, 0) + COALESCE(EXCLUDED.conbakemono, 0),
                mentir = COALESCE(incrementos_pendientes.mentir, 0) + COALESCE(EXCLUDED.mentir, 0),
                pilotear = COALESCE(incrementos_pendientes.pilotear, 0) + COALESCE(EXCLUDED.pilotear, 0),
                artesmarciales = COALESCE(incrementos_pendientes.artesmarciales, 0) + COALESCE(EXCLUDED.artesmarciales, 0),
                medicina = COALESCE(incrementos_pendientes.medicina, 0) + COALESCE(EXCLUDED.medicina, 0),
                conobjmagicos = COALESCE(incrementos_pendientes.conobjmagicos, 0) + COALESCE(EXCLUDED.conobjmagicos, 0),
                sigilo = COALESCE(incrementos_pendientes.sigilo, 0) + COALESCE(EXCLUDED.sigilo, 0),
                conesferas = COALESCE(incrementos_pendientes.conesferas, 0) + COALESCE(EXCLUDED.conesferas, 0),
                conleyendas = COALESCE(incrementos_pendientes.conleyendas, 0) + COALESCE(EXCLUDED.conleyendas, 0),
                forja = COALESCE(incrementos_pendientes.forja, 0) + COALESCE(EXCLUDED.forja, 0),
                condemonio = COALESCE(incrementos_pendientes.condemonio, 0) + COALESCE(EXCLUDED.condemonio, 0),
                conespiritual = COALESCE(incrementos_pendientes.conespiritual, 0) + COALESCE(EXCLUDED.conespiritual, 0),
                manejoblaster = COALESCE(incrementos_pendientes.manejoblaster, 0) + COALESCE(EXCLUDED.manejoblaster, 0),
                manejosombras = COALESCE(incrementos_pendientes.manejosombras, 0) + COALESCE(EXCLUDED.manejosombras, 0),
                tratobakemono = COALESCE(incrementos_pendientes.tratobakemono, 0) + COALESCE(EXCLUDED.tratobakemono, 0),
                conhechiceria = COALESCE(incrementos_pendientes.conhechiceria, 0) + COALESCE(EXCLUDED.conhechiceria, 0),

                medvital = COALESCE(incrementos_pendientes.medvital, 0) + COALESCE(EXCLUDED.medvital, 0),
                medespiritual = COALESCE(incrementos_pendientes.medespiritual, 0) + COALESCE(EXCLUDED.medespiritual, 0),
                rayo = COALESCE(incrementos_pendientes.rayo, 0) + COALESCE(EXCLUDED.rayo, 0),
                fuego = COALESCE(incrementos_pendientes.fuego, 0) + COALESCE(EXCLUDED.fuego, 0),
                frio = COALESCE(incrementos_pendientes.frio, 0) + COALESCE(EXCLUDED.frio, 0),
                veneno = COALESCE(incrementos_pendientes.veneno, 0) + COALESCE(EXCLUDED.veneno, 0),
                corte = COALESCE(incrementos_pendientes.corte, 0) + COALESCE(EXCLUDED.corte, 0),
                energia = COALESCE(incrementos_pendientes.energia, 0) + COALESCE(EXCLUDED.energia, 0),

                valcombate = COALESCE(incrementos_pendientes.valcombate, 0) + COALESCE(EXCLUDED.valcombate, 0),
                valcombate2 = COALESCE(incrementos_pendientes.valcombate2, 0) + COALESCE(EXCLUDED.valcombate2, 0),

                valadd1 = COALESCE(incrementos_pendientes.valadd1, 0) + COALESCE(EXCLUDED.valadd1, 0),
                valadd2 = COALESCE(incrementos_pendientes.valadd2, 0) + COALESCE(EXCLUDED.valadd2, 0),
                valadd3 = COALESCE(incrementos_pendientes.valadd3, 0) + COALESCE(EXCLUDED.valadd3, 0),
                valadd4 = COALESCE(incrementos_pendientes.valadd4, 0) + COALESCE(EXCLUDED.valadd4, 0)
            `,
            [
                incremento.idpersonaje, incremento.fuerza, incremento.fortaleza, incremento.destreza,
                incremento.agilidad, incremento.sabiduria, incremento.principio, incremento.presencia,
                incremento.sentidos, incremento.academisismo, incremento.alerta, incremento.atletismo,
                incremento.conbakemono, incremento.mentir, incremento.pilotear, incremento.artesmarciales,
                incremento.medicina, incremento.conobjmagicos, incremento.sigilo, incremento.conesferas,
                incremento.conleyendas, incremento.forja, incremento.condemonio, incremento.conespiritual,
                incremento.manejoblaster, incremento.manejosombras, incremento.tratobakemono,
                incremento.conhechiceria, incremento.medvital, incremento.medespiritual, incremento.rayo,
                incremento.fuego, incremento.frio, incremento.veneno, incremento.corte, incremento.energia,
                incremento.valcombate, incremento.valcombate2, incremento.valadd1, incremento.valadd2,
                incremento.valadd3, incremento.valadd4
            ]
        );

        // Avisar al jugador si está conectado
        console.log("EMITIENDO incrementos-pendientes para personaje:", incremento.idpersonaje);
        console.log("SOCKETS CONECTADOS:", io.engine.clientsCount);

        io.emit("incrementos_pendientes", {
            idpersonaje: incremento.idpersonaje
        });
        console.log("EMIT incrementos-pendientes EJECUTADO para personaje:", incremento.idpersonaje);

        return res.json({
            ok: true,
            mensaje: "Incremento del narrador guardado correctamente"
        });

    } catch (error) {
        console.log(error);
        return res.status(500).json({
            ok: false,
            error: error.message
        });
    }
});

app.post('/ConsumirIncremento', async (req, res) => {
    const { idpersonaje, campo, cantidad } = req.body;

  

    const camposPermitidos = [
        "fuerza",
        "fortaleza",
        "destreza",
        "agilidad",
        "sabiduria",
        "principio",
        "presencia",
        "sentidos",

        "academisismo",
        "alerta",
        "atletismo",
        "conbakemono",
        "mentir",
        "pilotear",
        "artesmarciales",
        "medicina",
        "conobjmagicos",
        "sigilo",
        "conesferas",
        "conleyendas",
        "forja",
        "condemonio",
        "conespiritual",
        "manejoblaster",
        "manejosombras",
        "tratobakemono",
        "conhechiceria",

        "medvital",
        "medespiritual",
        "rayo",
        "fuego",
        "frio",
        "veneno",
        "corte",
        "energia",

        "valcombate",
        "valcombate2",

        "valadd1",
        "valadd2",
        "valadd3",
        "valadd4",

        // KI
        "ki"
    ];

    try {

        if (!camposPermitidos.includes(campo)) {
            return res.status(400).json({
                ok: false,
                error: "Campo de incremento no válido"
            });
        }

        if (!Number.isInteger(idpersonaje) ||
            !Number.isInteger(cantidad) ||
            cantidad <= 0) {

            return res.status(400).json({
                ok: false,
                error: "Datos inválidos"
            });
        }

        const result = await pool.query(
            `
            UPDATE incrementos_pendientes
            SET ${campo} = ${campo} - $1
            WHERE idpersonaje = $2
              AND ${campo} >= $1
            RETURNING idpersonaje, ${campo}
            `,
            [
                cantidad,
                idpersonaje
            ]
        );

        if (result.rowCount === 0) {
            return res.status(400).json({
                ok: false,
                error: "No hay suficientes puntos de incremento para consumir"
            });
        }

        return res.json({
            ok: true,
            mensaje: "Incremento consumido correctamente",
            valor_restante: result.rows[0][campo]
        });

    } catch (error) {

        console.log(error);

        return res.status(500).json({
            ok: false,
            error: error.message
        });
    }
});


//CONSUMIR UN UNICO PERSONAJE 
// Consumir un personaje por ID
app.get('/consumirPersonaje/:idPersonaje', async (req, res) => {
  try {

    const { idPersonaje } = req.params;

    const userQuery = `
      SELECT 
        p.idpersonaje, p.nombre, p.dominio, p.raza, p.naturaleza, p.edad, p.ken, p.ki, p.destino, p."pDestino",
        p.fuerza, p.fortaleza, p.destreza, p.agilidad, p.sabiduria, p.presencia, p.principio,
        p.sentidos, p.academisismo, p.alerta, p.atletismo, p."conBakemono", p.mentir, p.pilotear,
        p."artesMarciales", p.medicina, p."conObjMagicos", p.sigilo, p."conEsferas", p."conLeyendas",
        p.forja, p."conDemonio", p."conEspiritual", p."manejoBlaster", p."manejoSombras", p."tratoBakemono",
        p."conHechiceria", p."medVital", p."medEspiritual", p.rayo, p.fuego, p.frio, p.veneno, p.corte,
        p.energia, p.ventajas, p."apCombate", p."valCombate", p."apCombate2", p."valCombate2",
        p.add1, p."valAdd1", p.add2, p."valAdd2", p.add3, p."valAdd3", p.add4, p."valAdd4",
        p.inventario, p.dominios, p."kenActual", p."kiActual", p.positiva, p.negativa, p."vidaActual",
        p.hechizos, p.consumision, p.iniciativa, p.historia, p."tecEspecial", p.conviccion, p.cicatriz,
        p.notasaga, p.resistencia, p."pjPnj", p.imagenurl, p.imagencloudid,
        p."imagenSeleccionada", p."coleccionImagenes", p.notas, p."usuarioId",

        a.spriteurl,
        a.filas,
        a.columnas,
        a.fps,
        a.scalesize,
        a.public_id

      FROM personajes p
      LEFT JOIN animaciones a
        ON a.idpersonaje = p.idpersonaje

      WHERE p.idpersonaje = $1
    `;

    const userResult = await pool.query(
      userQuery,
      [idPersonaje]
    );

    if (userResult.rows.length === 0) {
      return res.status(404).json({
        message: 'Personaje no encontrado'
      });
    }

    res.json(userResult.rows[0]);

  } catch (error) {
    console.error(
      'Error al obtener personaje:',
      error
    );

    res.status(500).json({
      message: 'Error en el servidor'
    });
  }
});







// ==========================================
// MIGRAR IMÁGENES DE PERSONAJES A IMAGEKIT
// ==========================================


//esto es el ultimo script que usamos para cargar de la base de datos pg a imagenkit
/*
async function migrarImagenesPersonajes() {

  const resultado = await pool.query(`
    SELECT idpersonaje, nombre, imagen
    FROM personajes
    WHERE imagenurl LIKE '%dzul1hatw%'
      AND imagen LIKE 'data:image/%;base64,%'
    ORDER BY idpersonaje
  `);

  const total = resultado.rows.length;

  let exitosas = 0;
  let fallidas = 0;

  console.log('\n================================');
  console.log('🚀 INICIANDO MIGRACIÓN IMAGEKIT');
  console.log('================================');
  console.log(`📊 Total encontradas: ${total}`);

  for (const personaje of resultado.rows) {

    try {

      console.log(
        `\n📤 [${exitosas + fallidas + 1}/${total}] ${personaje.nombre} (ID: ${personaje.idpersonaje})`
      );

      const subida = await subirImagenImageKit(
        personaje.imagen,
        '/personajes',
        `personaje_${personaje.idpersonaje}`
      );

      await pool.query(
        `UPDATE personajes
         SET imagenurl = $1,
             imagencloudid = $2
         WHERE idpersonaje = $3`,
        [
          subida.imagenurl,
          subida.imagencloudid,
          personaje.idpersonaje
        ]
      );

      exitosas++;

      console.log('✅ Migración exitosa');
      console.log('URL:', subida.imagenurl);
      console.log('File ID:', subida.imagencloudid);

    } catch (error) {

      fallidas++;

      console.error(
        `❌ Error en ${personaje.nombre} (ID: ${personaje.idpersonaje}):`,
        error.message
      );

    }
  }

  const pendientes = total - exitosas - fallidas;

  console.log('\n================================');
  console.log('🏁 RESUMEN DE MIGRACIÓN');
  console.log('================================');
  console.log(`📊 Total encontradas: ${total}`);
  console.log(`✅ Migradas correctamente: ${exitosas}`);
  console.log(`❌ Fallidas: ${fallidas}`);
  console.log(`⏳ Pendientes: ${pendientes}`);
  console.log('================================\n');

  return {
    total,
    exitosas,
    fallidas,
    pendientes
  };
}
migrarImagenesPersonajes().catch(console.error);
*/



server.listen(PORT, () => {
  console.log(`🟢 Servidor Socket.IO corriendo en puerto ${PORT}`);
});





