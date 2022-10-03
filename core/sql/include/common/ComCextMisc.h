
#ifndef COMCEXTMISC_H
#define COMCEXTMISC_H

#undef DLLEXPORT
#define DLLEXPORT 

#ifdef __cplusplus
extern "C" {

DLLEXPORT long long  TIME_SINCE_COLDLOAD (void);

DLLEXPORT void	TIME( short * a );

DLLEXPORT void	CONTIME( short * a, short t, short t1, short t2 );

DLLEXPORT void	DELAY (int      time);

}

#endif
#endif
