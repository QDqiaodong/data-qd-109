from datetime import date

def rebuild(events,as_of):
    active={};loans={};seen_time=set();future=0
    for e in events:
        if e['at']>as_of:future+=1;continue
        key=(e['copy_id'],e['at'])
        if key in seen_time:raise ValueError('ambiguous simultaneous events for copy')
        seen_time.add(key)
        copy=e['copy_id'];loan=e['loan_id'];kind=e['kind']
        if kind=='borrow':
            if copy in active:raise ValueError('copy already on loan')
            if loan in loans:raise ValueError('loan ID reused')
            state={'loan_id':loan,'copy_id':copy,'borrowed_at':e['at'],'due':e['due'],'returned_at':None,'renewals':0}
            loans[loan]=state;active[copy]=state
        else:
            if copy not in active or active[copy]['loan_id']!=loan:raise ValueError('no matching active loan')
            state=active[copy]
            if kind=='renew':
                if e['due']<=state['due']:raise ValueError('renewal must extend due date')
                state['due']=e['due'];state['renewals']+=1
            else:state['returned_at']=e['at'];del active[copy]
    for state in loans.values():
        end=(state['returned_at'] or as_of)[:10]
        state['overdue_days']=max(0,(date.fromisoformat(end)-date.fromisoformat(state['due'])).days)
        state['status']='closed' if state['returned_at'] else 'open'
    return sorted(loans.values(),key=lambda x:x['loan_id']),future
