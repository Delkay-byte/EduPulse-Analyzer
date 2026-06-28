export default function LoginScreen({ onLogin }: { onLogin: (role: 'Director' | 'Headteacher') => void }) {
  return (
    <div className="h-screen flex items-center justify-center bg-slate-950">
      <div className="bg-slate-900 p-8 rounded-2xl border border-slate-800 text-center max-w-sm">
        <h1 className="text-2xl font-black text-emerald-400 mb-2">EduPulse</h1>
        <p className="text-sm text-slate-400 mb-8">Select your role to continue</p>
        
        <div className="space-y-4">
          <button
            onClick={() => onLogin('Director')}
            className="w-full bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-3 rounded-lg transition"
          >
            Director Portal
          </button>
          <button
            onClick={() => onLogin('Headteacher')}
            className="w-full bg-blue-600 hover:bg-blue-500 text-white font-bold py-3 rounded-lg transition"
          >
            Headteacher Portal
          </button>
        </div>
      </div>
    </div>
  );
}
