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

/* Raw암호문:0xFF,0x12(2byte) -> Character 암호문으로 변환후 출력:"FF12"(4byte) */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Encrypt_Str( 
  unsigned char *contextBuf,        /*[IN]*/
  char          *plain,             /*[IN]*/
  unsigned int   plainLen,          /*[IN]*/
  char          *cipher,            /*[OUT]*/
  unsigned int  *cipherLen,         /*[OUT]*/
  unsigned int   cipherBufMax );    /*[IN]*/
/* Character 암호문입력:"FF12"(4byte) -> Raw 암호문0xFF,0x12(2byte)으로 변환후 복호화 */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Decrypt_Str( 
  unsigned char *contextBuf,        /*[IN]*/
  char          *cipher,            /*[IN]*/
  unsigned int   cipherLen,         /*[IN]*/
  char          *plain,             /*[OUT]*/
  unsigned int  *plainLen,          /*[OUT]*/
  unsigned int   plainBufMax );     /*[IN]*/
/* Raw암호문:0xFF,0x12(2byte) -> Base64Encode 한 값을 출력 */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Encrypt_B64(   
  unsigned char *contextBuf,        /*[IN]*/
  char          *plain,             /*[IN]*/
  unsigned int   plainLen,          /*[IN]*/
  char          *cipher,            /*[OUT]*/
  unsigned int  *cipherLen,         /*[OUT]*/
  unsigned int   cipherBufMax );    /*[IN]*/
/* Base64Encode 된 암호문:/xI=(4byte) -> Raw 암호문0xFF,0x12(2byte)으로 변환후 복호화 */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Decrypt_B64(   
  unsigned char *contextBuf,        /*[IN]*/
  char          *cipher,            /*[IN]*/
  unsigned int   cipherLen,         /*[OUT]*/
  char          *plain,             /*[OUT]*/
  unsigned int  *plainLen,          /*[OUT]*/
  unsigned int   plainBufMax );     /*[IN]*/
/* Raw암호문:0xFF,0x12(2byte) -> 그대로 출력:0xFF,0x12(2byte) */
LIBSPEC_SCPAPIDB 
int  
ScpAgt_Encrypt_Raw(   
  unsigned char *contextBuf,        /*[IN]*/
  unsigned char *plain,             /*[IN]*/
  unsigned int   plainLen,          /*[IN]*/
  unsigned char *cipher,            /*[OUT]*/
  unsigned int  *cipherLen,         /*[OUT]*/
  unsigned int   cipherBufMax );    /*[IN]*/
/* Raw암호문:0xFF,0x12(2byte) -> 그대로 복호화 */
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

/* inFilePath 의 파일을 outFilePath의 파일로 암호화한다.  
   outFileSize는 암호화된 파일의 사이즈이다. 
	 성공일 경우 리턴은 0, 실패시 -1이다
	 FIXED 모드만 지원한다.
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

/* inFilePath 의 파일을 outFilePath의 파일로 암호화한다.  
   outFileSize는 암호화된 파일의 사이즈이다. 
	 성공일 경우 리턴은 0, 실패시 -1이다
	 FIXED 모드만 지원한다.
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
/*  HASH 처리후 base64 인코딩하여 출력한다 */
LIBSPEC_SCPAPIDB
int 
ScpAgt_HASH_Base64(
  int            hashId,            /*[IN]*/
  unsigned char *input,             /*[IN]*/
  int            inputLen,          /*[IN]*/
  unsigned char *out,               /*[OUT]*/
  int           *outLen,            /*[OUT]*/
  int            outBufMax );       /*[IN]*/

/*  HASH 처리후 base64 인코딩하여 출력한다, ScpAgt_HASH_Base64와 동일함. */
LIBSPEC_SCPAPIDB
int 
ScpAgt_HASH_B64(
  int            hashId,            /*[IN]*/
  unsigned char *input,             /*[IN]*/
  int            inputLen,          /*[IN]*/
  unsigned char *out,               /*[OUT]*/
  int           *outLen,            /*[OUT]*/
  int            outBufMax );       /*[IN]*/

/*  HASH 처리후 Hexa String 처리하여 출력한다 */
LIBSPEC_SCPAPIDB
int 
ScpAgt_HASH_Str(
  int            hashId,            /*[IN]*/
  unsigned char *input,             /*[IN]*/
  int            inputLen,          /*[IN]*/
  unsigned char *out,               /*[OUT]*/
  int           *outLen,            /*[OUT]*/
  int            outBufMax );       /*[IN]*/

/*  HASH 처리후 그대로 출력한다 */
LIBSPEC_SCPAPIDB
int 
ScpAgt_HASH_Raw(
  int            hashId,            /*[IN]*/
  unsigned char *input,             /*[IN]*/
  int            inputLen,          /*[IN]*/
  unsigned char *out,               /*[OUT]*/
  int           *outLen,            /*[OUT]*/
  int            outBufMax );       /*[IN]*/

/* Hex 값을 입력 받아 Base64 인코딩 값을 출력한다. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_HexToB64( 
  char          *input,
  int            inputLen,
  char          *out,
  int           *outLen,
  int            outBufMax );

/* Base64 인코딩 값을 입력 받아 Hex 값을 출력한다. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_B64ToHex( 
  char          *input,
  int            inputLen,
  char          *out,
  int           *outLen,
  int            outBufMax );

/* Raw 값을 입력 받아 Base64 인코딩 값을 출력한다. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_RawToB64( 
  unsigned char *input,
  int            inputLen,
  char          *out,
  int           *outLen,
  int            outBufMax );

/* Base64 인코딩 값을 입력 받아 Raw 값을 출력한다. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_B64ToRaw( 
  char          *input,
  int            inputLen,
  unsigned char *out,
  int           *outLen,
  int            outBufMax );

/* Raw 값을 입력 받아 Hex 값을 출력한다. */
LIBSPEC_SCPAPIDB 
int 
ScpAgt_RawToHex( 
  unsigned char *input,
  int            inputLen,
  char          *out,
  int           *outLen,
  int            outBufMax );

/* Hex 값을 입력 받아 Raw 값을 출력한다. */
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

