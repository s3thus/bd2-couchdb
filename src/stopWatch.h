#pragma once
#include <time.h>
class stopWatch
{
public:
	stopWatch(void);
	~stopWatch(void);
	// get start time
	void startTimer(void);
	// get end time
	void stopTimer(void);
	// return event's duration
	double getDuration(void);
protected:
	// event's start time
	clock_t start;
	// event's stop time
	clock_t stop;
	// event's duration
	double duration;
	// calculate event's duration
	void calcDuration(void);
};

