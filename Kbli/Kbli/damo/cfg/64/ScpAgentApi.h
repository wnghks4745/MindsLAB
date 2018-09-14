#ifndef _SCP_AGENT_API_HEADER_
#define _SCP_AGENT_API_HEADER_

#ifdef LIBSPEC_SCPAPIDB
#undef LIBSPEC_SCPAPIDB
#endif

#if defined(EXPORT_SCPAPIDB)
#define LIBSPEC_SCPAPIDB __declspec(dllexport)
#elif defined(IMPORT_SCPAPIDB)
#define LIBSPEC_SCPAPIDB __declspec(dllimport)
#else
#define LIBSPEC_SCPAPIDB
#endif

#ifdef __cplusplus
extern "C" {
#endif

LIBSPEC_SCPAPIDB 
int  
ScpAgt_Init( const char *iniFilePath );

LIBSPEC_SCPAPIDB 
int  
ScpAgt_SetConfig( char *name, char *value );

LIBSPEC_SCPAPIDB 
int  
ScpAgt_ReInit( const char *iniFilePath );

LIBSPEC_SCPAPIDB 
int  
ScpAgt_Fini(void);

LIBSPEC_SCPAPIDB 
int
ScpAgt_CreateContextServiceID( 
  char          *serviceID,         /*[IN] serviceID*/
  char          *account,           /*[IN] db account*/
  unsigned char *outContextBuf,     /*[OUT] context*/
  int            outContextBufMax); /*[IN]*/

LIBSPEC_SCPAPIDB 
int
ScpAgt_CreateContext( 
  char          *agent_id,          /*[IN] agent id*/
  char          *db_name,           /*[IN] database name*/
  char          *db_owner,          /*[IN] db user name(schema name)*/
  char          *table_name,        /*[IN] table name*/
  char          *column_name,       /*[IN] column name*/
  unsigned char *outContextBuf,     /*[OUT] context*/  
  int            outContextBufMax); /*[IN]*/

LIBSPEC_SCPAPIDB 
int 
ScpAgt_CreateContextImportFile( 
  const char    *keyFilePath,       /*[IN]*/
  unsigned char *outContextBuf,     /*[OUT]*/   
  int            outContextBufMax); /*[IN]*/

LIBSPEC_SCPAPIDB 
int  
ScpAgt_DeleteContext( unsigned char *contextBuf);

LIBSPEC_SCPAPIDB 
int  
ScpAgt_ClearAllContext(void);

/* Raw��ȣ��:0xFF,0x12(2byte) -> Character ��ȣ������ ��ȯ�� ���:"FF12"(4byte) */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Encrypt_Str( 
  unsigned char *contextBuf,        /*[IN]*/
  char          *plain,             /*[IN]*/
  unsigned int   plainLen,          /*[IN]*/
  char          *cipher,            /*[OUT]*/
  unsigned int  *cipherLen,         /*[OUT]*/
  unsigned int   cipherBufMax );    /*[IN]*/
/* Character ��ȣ���Է�:"FF12"(4byte) -> Raw ��ȣ��0xFF,0x12(2byte)���� ��ȯ�� ��ȣȭ */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Decrypt_Str( 
  unsigned char *contextBuf,        /*[IN]*/
  char          *cipher,            /*[IN]*/
  unsigned int   cipherLen,         /*[IN]*/
  char          *plain,             /*[OUT]*/
  unsigned int  *plainLen,          /*[OUT]*/
  unsigned int   plainBufMax );     /*[IN]*/
/* Raw��ȣ��:0xFF,0x12(2byte) -> Base64Encode �� ���� ��� */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Encrypt_B64(   
  unsigned char *contextBuf,        /*[IN]*/
  char          *plain,             /*[IN]*/
  unsigned int   plainLen,          /*[IN]*/
  char          *cipher,            /*[OUT]*/
  unsigned int  *cipherLen,         /*[OUT]*/
  unsigned int   cipherBufMax );    /*[IN]*/
/* Base64Encode �� ��ȣ��:/xI=(4byte) -> Raw ��ȣ��0xFF,0x12(2byte)���� ��ȯ�� ��ȣȭ */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Decrypt_B64(   
  unsigned char *contextBuf,        /*[IN]*/
  char          *cipher,            /*[IN]*/
  unsigned int   cipherLen,         /*[OUT]*/
  char          *plain,             /*[OUT]*/
  unsigned int  *plainLen,          /*[OUT]*/
  unsigned int   plainBufMax );     /*[IN]*/
/* Raw��ȣ��:0xFF,0x12(2byte) -> �״�� ���:0xFF,0x12(2byte) */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Encrypt_Raw(   
  unsigned char *contextBuf,        /*[IN]*/
  unsigned char *plain,             /*[IN]*/
  unsigned int   plainLen,          /*[IN]*/
  unsigned char *cipher,            /*[OUT]*/
  unsigned int  *cipherLen,         /*[OUT]*/
  unsigned int   cipherBufMax );    /*[IN]*/
/* Raw��ȣ��:0xFF,0x12(2byte) -> �״�� ��ȣȭ */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Decrypt_Raw(   
  unsigned char *contextBuf,        /*[IN]*/
  unsigned char *cipher,            /*[IN]*/
  unsigned int   cipherLen,         /*[OUT]*/
  unsigned char *plain,             /*[OUT]*/
  unsigned int  *plainLen,          /*[OUT]*/
  unsigned int   plainBufMax );     /*[IN]*/
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Encrypt_Str_CP( 
  unsigned char *contextBuf,
  char          *plain,
  unsigned int   plainLen,
  char          *cipher,
  unsigned int  *cipherLen,
  unsigned int   cipherBufMax,
  char          *charset );
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Decrypt_Str_CP( 
  unsigned char *contextBuf,
  char          *cipher,
  unsigned int   cipherLen,
  char          *plain,
  unsigned int  *plainLen, 
  unsigned int   plainBufMax,
  char          *charset );

/* inFilePath �� ������ outFilePath�� ���Ϸ� ��ȣȭ�Ѵ�.  
   outFileSize�� ��ȣȭ�� ������ �������̴�. 
	 ������ ��� ������ 0, ���н� -1�̴�
	 FIXED ��常 �����Ѵ�.
*/
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Encrypt_FilePath( 
  unsigned char *contextBuf, 
  unsigned char *inFilePath,
  unsigned int   inFileSize,
  unsigned char *outFilePath,
  unsigned int  *outFileSize,
  unsigned int   outFileMax );

/* inFilePath �� ������ outFilePath�� ���Ϸ� ��ȣȭ�Ѵ�.  
   outFileSize�� ��ȣȭ�� ������ �������̴�. 
	 ������ ��� ������ 0, ���н� -1�̴�
	 FIXED ��常 �����Ѵ�.
*/
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Decrypt_FilePath(   
  unsigned char *contextBuf,
  unsigned char *inFilePath,
  unsigned int   inFileSize,
  unsigned char *outFilePath,
  unsigned int  *outFileSize, 
  unsigned int   outFileMax );

LIBSPEC_SCPAPIDB 
int  
ScpAgt_Encrypt_Str_Number( 
  unsigned char *contextBuf,
  char          *numberStr,
  unsigned int   numberStrLen,
  char          *cipher,
  unsigned int  *cipherLen,
  unsigned int   cipherBufMax, 
  char          *type );
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Decrypt_Str_Number( 
  unsigned char *contextBuf,
  char          *cipher,
  unsigned int   cipherLen,
  char          *numberStr,
  unsigned int  *numberStrLen,
  unsigned int   numberStrBufMax,  
  char          *type );

LIBSPEC_SCPAPIDB 
int  
ScpAgt_Encrypt_Str_Int( 
  unsigned char *contextBuf,
  int            number,
  char          *cipher,
  unsigned int  *cipherLen,
  unsigned int   cipherBufMax, 
  char          *type );
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Decrypt_Str_Int( 
  unsigned char *contextBuf,
  char          *cipher,
  unsigned int   cipherLen,
  int           *number,
  char          *type );
  
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Index_Str( 
  unsigned char *contextBuf,
  char          *plain,
  unsigned int   plainLen,
  char          *cipher,
  unsigned int  *cipherLen,
  unsigned int   cipherBufMax );
  
/* 
  HASH Algorithm ID :
  SHA1 = 70
  SHA256 = 71
  SHA384 = 72
  SHA512 = 73
  HAS160 = 74
  MD5 = 75  
*/
/*  HASH ó���� base64 ���ڵ��Ͽ� ����Ѵ� */
LIBSPEC_SCPAPIDB
int 
ScpAgt_HASH_Base64(
  int            hashId,            /*[IN]*/
  unsigned char *input,             /*[IN]*/
  int            inputLen,          /*[IN]*/
  unsigned char *out,               /*[OUT]*/
  int           *outLen,            /*[OUT]*/
  int            outBufMax );       /*[IN]*/

/*  HASH ó���� base64 ���ڵ��Ͽ� ����Ѵ�, ScpAgt_HASH_Base64�� ������. */
LIBSPEC_SCPAPIDB
int 
ScpAgt_HASH_B64(
  int            hashId,            /*[IN]*/
  unsigned char *input,             /*[IN]*/
  int            inputLen,          /*[IN]*/
  unsigned char *out,               /*[OUT]*/
  int           *outLen,            /*[OUT]*/
  int            outBufMax );       /*[IN]*/

/*  HASH ó���� Hexa String ó���Ͽ� ����Ѵ� */
LIBSPEC_SCPAPIDB
int 
ScpAgt_HASH_Str(
  int            hashId,            /*[IN]*/
  unsigned char *input,             /*[IN]*/
  int            inputLen,          /*[IN]*/
  unsigned char *out,               /*[OUT]*/
  int           *outLen,            /*[OUT]*/
  int            outBufMax );       /*[IN]*/

/*  HASH ó���� �״�� ����Ѵ� */
LIBSPEC_SCPAPIDB
int 
ScpAgt_HASH_Raw(
  int            hashId,            /*[IN]*/
  unsigned char *input,             /*[IN]*/
  int            inputLen,          /*[IN]*/
  unsigned char *out,               /*[OUT]*/
  int           *outLen,            /*[OUT]*/
  int            outBufMax );       /*[IN]*/

/* Hex ���� �Է� �޾� Base64 ���ڵ� ���� ����Ѵ�. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_HexToB64( 
  char          *input,
  int            inputLen,
  char          *out,
  int           *outLen,
  int            outBufMax );

/* Base64 ���ڵ� ���� �Է� �޾� Hex ���� ����Ѵ�. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_B64ToHex( 
  char          *input,
  int            inputLen,
  char          *out,
  int           *outLen,
  int            outBufMax );

/* Raw ���� �Է� �޾� Base64 ���ڵ� ���� ����Ѵ�. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_RawToB64( 
  unsigned char *input,
  int            inputLen,
  char          *out,
  int           *outLen,
  int            outBufMax );

/* Base64 ���ڵ� ���� �Է� �޾� Raw ���� ����Ѵ�. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_B64ToRaw( 
  char          *input,
  int            inputLen,
  unsigned char *out,
  int           *outLen,
  int            outBufMax );

/* Raw ���� �Է� �޾� Hex ���� ����Ѵ�. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_RawToHex( 
  unsigned char *input,
  int            inputLen,
  char          *out,
  int           *outLen,
  int            outBufMax );

/* Hex ���� �Է� �޾� Raw ���� ����Ѵ�. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_HexToRaw( 
  char          *input,
  int            inputLen,
  unsigned char *out,
  int           *outLen,
  int            outBufMax );

LIBSPEC_SCPAPIDB 
char* ScpAgt_GetVersion();

LIBSPEC_SCPAPIDB 
int 
SCP_ExportContext(
  char          *iniFilePath,
  char          *iniKeyName,
  char          *outContextStr,
  unsigned int  *outContextStrLen,
  unsigned int   outContextStrMax);

LIBSPEC_SCPAPIDB 
int 
SCP_ExportKey(
  char          *iniFilePath,
  char          *serviceID,
  char          *account,
  char          *outKeyStr,
  unsigned int  *outKeyStrLen,
  unsigned int   outKeyStrMax);

LIBSPEC_SCPAPIDB 
int  
SCP_EncStr(
  char          *iniFilePath,
  char          *iniKeyName, /* or outContextStr */
  char          *plain,
  unsigned int   plainLen,
  char          *cipher,
  unsigned int  *cipherLen,
  unsigned int   cipherBufMax );

LIBSPEC_SCPAPIDB 
int 
SCP_DecStr( 
  char          *iniFilePath,
  char          *iniKeyName, /* or outContextStr */
  char          *cipher,
  unsigned int   cipherLen,
  char          *plain,
  unsigned int  *plainLen,
  unsigned int   plainBufMax );

LIBSPEC_SCPAPIDB 
int  
SCP_EncB64(
  char          *iniFilePath,
  char          *iniKeyName, /* or outContextStr */
  char          *plain,
  unsigned int   plainLen,
  char          *cipher,
  unsigned int  *cipherLen,
  unsigned int   cipherBufMax );

LIBSPEC_SCPAPIDB 
int 
SCP_DecB64( 
  char          *iniFilePath,
  char          *iniKeyName, /* or outContextStr */
  char          *cipher,
  unsigned int   cipherLen,
  char          *plain,
  unsigned int  *plainLen,
  unsigned int   plainBufMax );

LIBSPEC_SCPAPIDB 
int  
SCP_EncFile(
  char          *iniFilePath,
  char          *iniKeyName, /* or outContextStr */
  char          *inFilePath,
  char          *outFilePath );

LIBSPEC_SCPAPIDB 
int 
SCP_DecFile( 
  char          *iniFilePath,
  char          *iniKeyName, /* or outContextStr */
  char          *inFilePath,
  char          *outFilePath );

#ifdef __cplusplus
}
#endif

#endif //_SCP_AGENT_API_HEADER_

